/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Keeps track of all the static yml settings for search path
 */
public class SearchStaticSettings {

    public static final String CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE =
        "opensearch.experimental.feature.concurrent_segment_search.max_slice";

    public static final Setting<Integer> CONCURRENT_SEGMENT_SEARCH_SETTING = Setting.intSetting(
        CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE,
        -1,
        Setting.Property.NodeScope
    );
    private static Settings settings;

    public static void initializeSettings(Settings openSearchSettings) {
        settings = openSearchSettings;
    }

    public static int getSettingValue() {
        return (settings != null) ? settings.getAsInt(CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE, -1) : -1;
    }
}
