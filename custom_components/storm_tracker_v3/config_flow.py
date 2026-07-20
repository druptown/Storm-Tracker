"""Configuratieflow voor Storm Tracker V3."""
from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import DOMAIN

CONF_PERSONS = "persons"
CONF_TEST_TRACKER = "test_tracker_entity"
CONF_RADAR_RADIUS = "radar_radius_km"
CONF_SHARING_DISTANCE = "engine_sharing_distance_km"
CONF_EUMETSAT_KEY = "eumetsat_consumer_key"
CONF_EUMETSAT_SECRET = "eumetsat_consumer_secret"
CONF_LIGHTNING_SOURCE_MODE = "lightning_source_mode"


def _schema(defaults: dict) -> vol.Schema:
    fields = {
        vol.Optional(CONF_PERSONS, default=defaults.get(CONF_PERSONS, [])):
            selector.EntitySelector(selector.EntitySelectorConfig(
                domain="device_tracker", integration="life360", multiple=True,
            )),
        vol.Optional(CONF_RADAR_RADIUS, default=defaults.get(CONF_RADAR_RADIUS, 300.0)):
            selector.NumberSelector(selector.NumberSelectorConfig(
                min=50, max=1000, step=10, mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="km",
            )),
        vol.Optional(
            CONF_SHARING_DISTANCE,
            default=defaults.get(CONF_SHARING_DISTANCE, 150.0),
        ): selector.NumberSelector(selector.NumberSelectorConfig(
            min=25, max=500, step=25, mode=selector.NumberSelectorMode.BOX,
            unit_of_measurement="km",
        )),
        vol.Optional(
            CONF_EUMETSAT_KEY,
            default=defaults.get(CONF_EUMETSAT_KEY, ""),
        ): selector.TextSelector(),
        vol.Optional(
            CONF_EUMETSAT_SECRET,
            default=defaults.get(CONF_EUMETSAT_SECRET, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_LIGHTNING_SOURCE_MODE,
            default=defaults.get(CONF_LIGHTNING_SOURCE_MODE, "auto"),
        ): selector.SelectSelector(selector.SelectSelectorConfig(
            options=["auto", "satellite_test"],
            mode=selector.SelectSelectorMode.DROPDOWN,
            translation_key="lightning_source_mode",
        )),
    }
    test_selector = selector.EntitySelector(selector.EntitySelectorConfig(
        domain="device_tracker",
    ))
    if defaults.get(CONF_TEST_TRACKER):
        fields[vol.Optional(
            CONF_TEST_TRACKER, default=defaults[CONF_TEST_TRACKER]
        )] = test_selector
    else:
        fields[vol.Optional(CONF_TEST_TRACKER)] = test_selector
    return vol.Schema(fields)


class StormTrackerConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Maak precies een Storm Tracker-installatie aan."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if user_input is not None:
            return self.async_create_entry(title="Storm Tracker V3", data=user_input)
        return self.async_show_form(step_id="user", data_schema=_schema({}))

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return StormTrackerOptionsFlow()


class StormTrackerOptionsFlow(config_entries.OptionsFlow):
    """Beheer Life360-personen en de optionele testtracker."""

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        defaults = {**self.config_entry.data, **self.config_entry.options}
        return self.async_show_form(step_id="init", data_schema=_schema(defaults))
