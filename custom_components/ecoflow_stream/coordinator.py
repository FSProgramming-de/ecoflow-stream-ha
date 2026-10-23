"""EcoFlow Stream Datenkoordinator."""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import EcoFlowApiClient, EcoFlowMqttClient
from .const import (
    DOMAIN,
    SENSOR_DEFINITIONS,
    BINARY_SENSOR_DEFINITIONS,
    IGNORED_PARAMS,
    UNKNOWN_PARAMS_FILE,
    PV_ENERGY_FILE,
    PER_DEVICE_KEYS,
)

_LOGGER = logging.getLogger(__name__)

# PV-Energiezähler höchstens so oft auf die Platte schreiben (Sekunden)
PV_SAVE_INTERVAL = 60

# Historische Energie-Codes (BKW = Balkonkraftwerk)
ENERGY_CODES = {
    "solar_energy_wh": "BK621-App-HOME-SOLAR-ENERGY-FLOW-solor-line-NOTDISTINGUISH-MASTER_DATA",
    "load_energy_wh": "BK621-App-HOME-LOAD-ENERGY-FLOW-consumption-prop_arc-NOTDISTINGUISH-MASTER_DATA",
    "grid_energy_wh": "BK621-App-HOME-GRID-ENERGY-FLOW-grid_prop_bar-NOTDISTINGUISH-MASTER_DATA",
    "battery_energy_wh": "BK621-App-HOME-SOC-ENERGY-FLOW-battery-prop_bar-NOTDISTINGUISH-MASTER_DATA",
    "independence_pct": "BK621-App-HOME-INDEPENDENCE-PERCENT-FLOW-indep-progress_bar-NOTDISTINGUISH-MASTER_DATA",
}


def _read_json(path: str) -> dict | None:
    """JSON-Datei lesen (blockierend – nur im Executor aufrufen)."""
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)


def _write_json(path: str, data: dict, indent: int | None = None) -> None:
    """JSON-Datei schreiben (blockierend – nur im Executor aufrufen)."""
    with open(path, "w") as f:
        json.dump(data, f, indent=indent, default=str)


class EcoFlowCoordinator(DataUpdateCoordinator):
    """Koordiniert REST-Polling + MQTT-Push."""

    def __init__(
        self,
        hass: HomeAssistant,
        api: EcoFlowApiClient,
        main_sn: str,
        secondary_sn: str | None,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=30),
        )
        self.api = api
        self.main_sn = main_sn
        self.secondary_sn = secondary_sn
        self.mqtt_client: EcoFlowMqttClient | None = None

        # Echtzeit-Daten (MQTT)
        self.realtime_data: dict[str, Any] = {}
        # Tagesdaten (REST historical)
        self.energy_today: dict[str, Any] = {}
        # Tag (UTC), für den energy_today gilt → Grundlage für last_reset der Tagessensoren
        self.energy_today_start: datetime | None = None
        # Alle unbekannten Parameter speichern
        self._unknown_params: dict[str, Any] = {}
        self._unknown_params_path = hass.config.path(UNKNOWN_PARAMS_FILE)

        # Kumulativer PV-Energiezähler
        self._pv_energy_path = hass.config.path(PV_ENERGY_FILE)
        self._pv_energy_wh: float = 0.0
        self._last_pv_update: datetime | None = None
        self._last_pv_save: float = 0.0

    async def async_load_storage(self) -> None:
        """Gespeicherte Zähler und unbekannte Parameter laden (Dateizugriff im Executor)."""
        try:
            data = await self.hass.async_add_executor_job(_read_json, self._pv_energy_path)
            if data:
                self._pv_energy_wh = float(data.get("pv_energy_wh", 0.0))
                _LOGGER.debug("PV-Energiezähler geladen: %.1f Wh", self._pv_energy_wh)
            # Gespeicherten Stand sofort melden – sonst bleibt der Sensor nach einem Neustart
            # ohne PV-Leistung (z. B. nachts) bis zum nächsten Sonnenaufgang „nicht verfügbar“
            if self._pv_energy_wh > 0:
                self.realtime_data["pv_energy_total_wh"] = round(self._pv_energy_wh, 1)
        except Exception as ex:
            _LOGGER.warning("PV-Energiezähler konnte nicht geladen werden: %s", ex)
        try:
            data = await self.hass.async_add_executor_job(_read_json, self._unknown_params_path)
            if data:
                self._unknown_params = data
                _LOGGER.debug("Unbekannte Parameter geladen: %d", len(self._unknown_params))
        except Exception as ex:
            _LOGGER.warning("Unbekannte Parameter konnten nicht geladen werden: %s", ex)

    async def _async_save(self, path: str, data: dict, what: str, indent: int | None = None) -> None:
        try:
            await self.hass.async_add_executor_job(_write_json, path, data, indent)
        except Exception as ex:
            _LOGGER.warning("%s konnte nicht gespeichert werden: %s", what, ex)

    def _save_pv_energy(self, force: bool = False) -> None:
        """PV-Energiezähler speichern – höchstens alle PV_SAVE_INTERVAL Sekunden (läuft im Event-Loop)."""
        now = time.monotonic()
        if not force and now - self._last_pv_save < PV_SAVE_INTERVAL:
            return
        self._last_pv_save = now
        self.hass.async_create_task(
            self._async_save(self._pv_energy_path, {"pv_energy_wh": self._pv_energy_wh}, "PV-Energiezähler")
        )

    def _save_unknown_params(self) -> None:
        """Unbekannte Parameter speichern (läuft im Event-Loop)."""
        self.hass.async_create_task(
            self._async_save(self._unknown_params_path, dict(self._unknown_params), "Unbekannte Parameter", 2)
        )

    def _update_system_soc(self) -> None:
        """Gewichteten Gesamt-SOC berechnen.

        Ultra X:  3.840 Wh → MQTT Key: 'soc' von BK61ZE1B2J1F0293
        AC Pro:   1.920 Wh → MQTT Key: 'cmsBattSoc' von BK31ZE1A4H4J1395
        Gesamt:   5.760 Wh
        """
        soc_ultrax = self.realtime_data.get("ultrax_soc")   # bmsBattSoc von BK61
        soc_acpro = self.realtime_data.get("acpro_soc")      # soc von BK31

        if soc_ultrax is not None and soc_acpro is not None:
            try:
                wh_ultrax = 3840.0
                wh_acpro = 1920.0
                wh_total = wh_ultrax + wh_acpro
                soc_weighted = (float(soc_ultrax) * wh_ultrax + float(soc_acpro) * wh_acpro) / wh_total
                self.realtime_data["soc_system_weighted"] = round(soc_weighted, 1)
            except (ValueError, TypeError) as ex:
                _LOGGER.debug("SOC Berechnung fehlgeschlagen: %s", ex)

    def _update_pv_energy(self, params: dict) -> None:
        """PV-Energie aus aktueller MQTT-Nachricht hochzählen."""
        # PV-Gesamtleistung aus powGetPvSum (AC Pro Master) oder Summe der MPPTs
        pv_power = 0.0
        if "powGetPvSum" in params:
            pv_power = float(params["powGetPvSum"])
        else:
            for key in ["powGetPv", "powGetPv2", "powGetPv3", "powGetPv4"]:
                if key in params:
                    pv_power += float(params[key])

        if pv_power <= 0:
            return

        now = datetime.now()
        if self._last_pv_update is not None:
            elapsed_seconds = (now - self._last_pv_update).total_seconds()
            # Maximal 120 Sekunden berücksichtigen (verhindert Sprünge nach Verbindungsabbruch)
            elapsed_seconds = min(elapsed_seconds, 120)
            energy_delta_wh = pv_power * elapsed_seconds / 3600
            self._pv_energy_wh += energy_delta_wh
            self._save_pv_energy()

        self._last_pv_update = now
        self.realtime_data["pv_energy_total_wh"] = round(self._pv_energy_wh, 1)

    def _process_mqtt_params(self, sn: str, params: dict) -> None:
        """MQTT-Nachricht verarbeiten und unbekannte Parameter erkennen (läuft im Event-Loop)."""
        known_keys = set(SENSOR_DEFINITIONS.keys()) | set(BINARY_SENSOR_DEFINITIONS.keys()) | IGNORED_PARAMS
        known_keys |= {"cloudMetter_phaseAPower", "cloudMetter_phaseBPower", "cloudMetter_phaseCPower"}

        new_unknown = False
        for key, value in params.items():
            # cloudMetter aufdröseln
            if key == "cloudMetter" and isinstance(value, dict):
                self.realtime_data["cloudMetter_phaseAPower"] = value.get("phaseAPower", 0)
                self.realtime_data["cloudMetter_phaseBPower"] = value.get("phaseBPower", 0)
                self.realtime_data["cloudMetter_phaseCPower"] = value.get("phaseCPower", 0)
                continue

            # bmsBattSoc kommt von BEIDEN Geräten mit ihrem jeweiligen SOC
            if key == "bmsBattSoc":
                if sn == self.main_sn:
                    self.realtime_data["ultrax_soc"] = value   # Ultra X SOC
                elif sn == self.secondary_sn:
                    self.realtime_data["acpro_soc"] = value    # AC Pro SOC
                continue

            # Zähler, die beide Geräte unter demselben Key senden, getrennt ablegen
            if key in PER_DEVICE_KEYS:
                if sn == self.main_sn:
                    self.realtime_data[key] = value
                elif sn == self.secondary_sn:
                    self.realtime_data[f"acpro_{key}"] = value
                continue

            # cmsBattSoc = Gesamt-System-SOC, kommt vom aktuellen MASTER
            # Master kann wechseln (Ultra X oder AC Pro) -> nur Wert > 0 nehmen
            if key == "cmsBattSoc":
                if float(value) > 0:
                    self.realtime_data["system_soc_cms"] = value
                continue

            # soc ignorieren (identisch mit bmsBattSoc)
            if key == "soc":
                continue

            # Normalen Wert speichern
            self.realtime_data[key] = value

            # Unbekannte Parameter erkennen
            if key not in known_keys and not isinstance(value, (dict, list)):
                if key not in self._unknown_params:
                    _LOGGER.info("Neuer unbekannter Parameter entdeckt: %s = %s (SN: %s)", key, value, sn)
                    new_unknown = True
                self._unknown_params[key] = {
                    "value": value,
                    "sn": sn,
                    "last_seen": datetime.now().isoformat(),
                }

        if new_unknown:
            self._save_unknown_params()

        # PV-Energiezähler aktualisieren
        self._update_pv_energy(params)

        # Gewichteten Gesamt-SOC berechnen
        self._update_system_soc()

        # HA Update anstoßen
        self.async_set_updated_data(self._get_combined_data())

    def _get_combined_data(self) -> dict:
        return {
            "realtime": self.realtime_data,
            "energy_today": self.energy_today,
        }

    async def setup_mqtt(self) -> None:
        """MQTT Client einrichten und starten."""
        try:
            creds = await self.api.get_mqtt_credentials()
            if not creds:
                _LOGGER.error("Keine MQTT Credentials erhalten")
                return

            sn_list = [self.main_sn]
            if self.secondary_sn:
                sn_list.append(self.secondary_sn)

            self.mqtt_client = EcoFlowMqttClient(
                cert_account=creds["certificateAccount"],
                cert_password=creds["certificatePassword"],
                sn_list=sn_list,
                on_message_callback=self._process_mqtt_params,
            )
            # TLS-Setup und Verbindungsaufbau blockieren – deshalb im Executor
            await self.hass.async_add_executor_job(self.mqtt_client.start, self.hass.loop)
            _LOGGER.info("MQTT für %d Gerät(e) gestartet", len(sn_list))
        except Exception as ex:
            _LOGGER.error("MQTT Setup Fehler: %s", ex)

    async def _async_update_data(self) -> dict:
        """REST-Polling: Quota + historische Tagesdaten."""
        try:
            # Quota abrufen (15 offizielle Felder)
            quota = await self.api.get_all_quota(self.main_sn)
            for key, value in quota.items():
                self.realtime_data[key] = value  # Quota gilt nur für das Hauptgerät

            # Historische Tagesdaten abrufen
            await self._fetch_energy_today()

            return self._get_combined_data()
        except Exception as ex:
            raise UpdateFailed(f"EcoFlow Update Fehler: {ex}") from ex

    async def _fetch_energy_today(self) -> None:
        """Energie-Tagesdaten abrufen."""
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        begin = now.strftime("%Y-%m-%d 00:00:00")
        end = now.strftime("%Y-%m-%d 23:59:59")

        values: dict[str, Any] = {}
        for metric_key, code in ENERGY_CODES.items():
            try:
                result = await self.api.get_historical_data(
                    self.main_sn, code, begin, end
                )
                if result:
                    # Summiere alle indexValues des Tages
                    total = sum(
                        float(item.get("indexValue", 0))
                        for item in result
                        if item.get("indexValue") is not None
                    )
                    values[metric_key] = total
            except Exception as ex:
                _LOGGER.debug("Tagesdaten für %s nicht verfügbar: %s", metric_key, ex)

        if self.energy_today_start != day_start:
            # Neuer Tag: alte Tageswerte verwerfen, damit kein Wert von gestern mit dem neuen last_reset gemeldet wird
            self.energy_today.clear()
            self.energy_today_start = day_start
        self.energy_today.update(values)

    def get_unknown_params(self) -> dict:
        """Alle unbekannten Parameter zurückgeben."""
        return self._unknown_params.copy()

    def stop(self) -> None:
        """MQTT stoppen und PV-Zähler sichern (blockierend – im Executor aufrufen)."""
        if self.mqtt_client:
            self.mqtt_client.stop()
        try:
            _write_json(self._pv_energy_path, {"pv_energy_wh": self._pv_energy_wh})
        except Exception as ex:
            _LOGGER.warning("PV-Energiezähler konnte nicht gespeichert werden: %s", ex)
