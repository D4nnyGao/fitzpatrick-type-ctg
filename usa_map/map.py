import json
import re
import time
import os
import pandas as pd
import requests
import folium
from collections import defaultdict
from branca.element import Element

# --- Configuration ---
API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
SEARCH_KEYWORD = 'fitzpatrick'
COUNTRY_TO_ISOLATE = "United States"

RAW_JSON_FILENAME = "usa_map/fitzpatrick_usa_search.json"
FINAL_OUTPUT_CSV = "usa_map/usa_fitzpatrick_trials_dataset.csv"
MAP_OUTPUT_HTML = "usa_map/fitzpatrick_studies_map.html"

# --- Filter Configuration ---
FILTERS = {
    'apply_filters': False, 
    'required_skin_types': [],
    'min_participants_by_race': {
        'Race_Black_or_African_American': 0,
        'Race_White': 0,
        'Race_Asian': 0,
    }
}


def fetch_clinical_trials_data(api_url, keyword, output_filename):
    """
    Searches the ClinicalTrials.gov API for studies matching a keyword
    and saves the raw results to a JSON file.
    """
    if os.path.exists(output_filename):
        print(f"[*] Raw data file '{output_filename}' already exists. Skipping download.")
        return

    all_studies = []
    page_count = 1
    next_page_token = None
    eligibility_search = f'AREA[EligibilityCriteria]({keyword}) AND SEARCH[Location](AREA[LocationCountry]"{COUNTRY_TO_ISOLATE}")'
    fields_to_get = ["NCTId", "protocolSection", "resultsSection"]
    params = {'query.term': eligibility_search, 'fields': ",".join(fields_to_get), 'pageSize': 100}

    print(f"[*] Starting API query to fetch clinical trial data for country: {COUNTRY_TO_ISOLATE}...")
    while True:
        try:
            if next_page_token:
                params['pageToken'] = next_page_token
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            current_studies = data.get('studies', [])
            if not current_studies: break
            all_studies.extend(current_studies)
            print(f"[*] Page {page_count}: Fetched {len(current_studies)} studies. Total so far: {len(all_studies)}")
            next_page_token = data.get('nextPageToken')
            if not next_page_token: break
            page_count += 1
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            print(f"\n[!] API request failed on page {page_count}: {e}")
            break

    if all_studies:
        print(f"\n[*] Saving {len(all_studies)} total studies to '{output_filename}'...")
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump({'studies': all_studies}, f, ensure_ascii=False, indent=2)
        print(f"[*] Successfully saved raw data.")
    else:
        print("\n[!] No studies were found to save.")


def parse_eligibility_criteria(study_record, keyword):
    """Finds sentences mentioning a keyword in the eligibility criteria."""
    eligibility_text = study_record.get('protocolSection', {}).get('eligibilityModule', {}).get('eligibilityCriteria', '')
    if not eligibility_text: return []
    parts = re.split(r'exclusion', eligibility_text, flags=re.IGNORECASE)
    found_sentences = []
    for text_part, is_exclusion in [(parts[0], False), (parts[1] if len(parts) > 1 else "", True)]:
        if not text_part: continue
        for sentence in re.split(r'[.\n]', text_part):
            if keyword in sentence.lower() and sentence.strip():
                found_sentences.append({'sentence': sentence.strip(), 'is_exclusion': is_exclusion})
    return found_sentences


def extract_and_standardize_scores(sentence):
    """Analyzes a sentence to extract and format Fitzpatrick scores."""
    if not isinstance(sentence, str): return {}
    text = sentence.lower()
    result = {'extracted_score': 'Not Specified', 'Type_I': 0, 'Type_II': 0, 'Type_III': 0, 'Type_IV': 0, 'Type_V': 0, 'Type_VI': 0}
    if any(word in text for word in ['wrinkle']):
        result['extracted_score'] = 'Not a Skin Type Score'
        return result
    if 'all' in text or 'any' in text:
        result.update({k: 1 for k in result if k.startswith('Type_')})
        result['extracted_score'] = 'All'
        return result

    roman_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6}
    to_roman_map = {v: k for k, v in roman_map.items()}
    
    def _to_int(s):
        s_upper = s.upper()
        if s_upper == 'L':
            return 1
        return int(s) if s.isdigit() else roman_map.get(s_upper)

    valid_numeral_pattern = r'\b(vi|v|iv|iii|ii|i|l|[1-6])\b'
    range_pattern = f'{valid_numeral_pattern}\\s*(?:-|to|through)\\s*{valid_numeral_pattern}'
    range_match = re.search(range_pattern, text, re.IGNORECASE)

    if range_match:
        start_str, end_str = range_match.groups()
        start_num, end_num = _to_int(start_str), _to_int(end_str)
        if start_num is not None and end_num is not None and start_num < end_num:
            for i in range(start_num, end_num + 1):
                if i in to_roman_map: result[f"Type_{to_roman_map[i]}"] = 1
            result['extracted_score'] = f"{to_roman_map.get(start_num, '')}-{to_roman_map.get(end_num, '')}"
    else:
        numerals_found = re.findall(valid_numeral_pattern, text, re.IGNORECASE)
        scores = sorted(list(set(_to_int(n) for n in numerals_found if _to_int(n) is not None)))
        roman_scores = []
        for score in scores:
            roman_version = to_roman_map.get(score)
            if roman_version:
                result[f"Type_{roman_version}"] = 1
                roman_scores.append(roman_version)
        if roman_scores: result['extracted_score'] = ", ".join(roman_scores)
    return result


def extract_study_details(study_record, country):
    """Extracts status, enrollment, US locations with coordinates, and race demographics."""
    details = {'status': "N/A", 'us_facilities': [], 'enrollment': 'N/A', 'enrollment_type': 'N/A', 'race_data': {}}
    protocol = study_record.get('protocolSection', {})
    if not protocol: return details
    
    details['status'] = protocol.get('statusModule', {}).get('overallStatus', 'N/A')

    enrollment_info = protocol.get('designModule', {}).get('enrollmentInfo')
    if enrollment_info and 'count' in enrollment_info:
        details['enrollment'] = enrollment_info['count']
        details['enrollment_type'] = enrollment_info.get('type', 'N/A')

    for loc in protocol.get('contactsLocationsModule', {}).get('locations', []):
        if loc.get('country') == country and loc.get('geoPoint'):
            details['us_facilities'].append({
                'facility': loc.get('facility', 'N/A'), 'city': loc.get('city', 'N/A'),
                'state': loc.get('state', 'N/A'), 'zip': loc.get('zip', 'N/A'),
                'latitude': loc.get('geoPoint', {}).get('lat'), 'longitude': loc.get('geoPoint', {}).get('lon')
            })

    # Extract race demographics ONCE per study (not per facility)
    results_section = study_record.get('resultsSection', {})
    if results_section:
        for measure in results_section.get('baselineCharacteristicsModule', {}).get('measures', []):
            if measure.get('title') == "Race (NIH/OMB)":
                for cat in measure.get('classes', [{}])[0].get('categories', []):
                    race_title = cat.get('title')
                    if race_title:
                        total_count = sum(int(m.get('value', 0)) for m in cat.get('measurements', []))
                        details['race_data'][f"Race_{race_title.replace(' ', '_')}"] = total_count
    return details
def create_interactive_map_with_sidebar(map_data, filename):
    """Creates an interactive Folium map with a custom sidebar interface,
    using single-color, dynamically-sized markers."""
    if not map_data:
        print("[!] No data available to create a map.")
        return

    us_center = [39.8283, -98.5795]
    m = folium.Map(location=us_center, zoom_start=4, tiles="cartodbpositron")

    print(f"[*] Generating map with interactive sidebar from {len(map_data)} records...")
    
    locations_data = defaultdict(list)
    for record in map_data:
        lat, lon = record.get('latitude'), record.get('longitude')
        if lat is not None and lon is not None:
            key = f"{float(lat):.6f},{float(lon):.6f}"
            locations_data[key].append(record)

    all_race_columns = sorted([col for col in map_data[0].keys() if col.startswith('Race_')]) if map_data else []
    
    enrollment_values = [r.get('enrollment') for r in map_data if isinstance(r.get('enrollment'), (int, float)) and r.get('enrollment') > 0]
    max_enrollment = max(enrollment_values) if enrollment_values else 1000
    
    all_statuses = sorted(set(r.get('status', 'N/A') for r in map_data))
    status_display_map = {
        'ACTIVE_NOT_RECRUITING': 'Active, not recruiting',
        'COMPLETED': 'Completed',
        'ENROLLING_BY_INVITATION': 'Enrolling by invitation',
        'NOT_YET_RECRUITING': 'Not yet recruiting',
        'RECRUITING': 'Recruiting',
        'SUSPENDED': 'Suspended',
        'TERMINATED': 'Terminated',
        'WITHDRAWN': 'Withdrawn',
        'AVAILABLE': 'Available',
        'NO_LONGER_AVAILABLE': 'No longer available',
        'TEMPORARILY_NOT_AVAILABLE': 'Temporarily not available',
        'APPROVED_FOR_MARKETING': 'Approved for marketing',
        'WITHHELD': 'Withheld',
        'UNKNOWN': 'Unknown status',
        'N/A': 'N/A'
    }
    
    total_studies = len(set(record['nctId'] for record in map_data))
    total_locations = len(locations_data)
    
    race_data = {}
    for col in all_race_columns:
        values = [r.get(col, 0) for r in map_data if isinstance(r.get(col), (int, float)) and r.get(col) > 0]
        if values: 
            race_data[col] = {
                'min': 0, 
                'max': max(values), 
                'display_name': col.replace('Race_', '').replace('_', ' ')
            }
    
    css_rules = """
        body { margin: 0; padding: 0; font-family: 'Segoe UI', sans-serif; }
        .sidebar { 
            position: fixed; top: 0; left: 0; width: 320px; height: 100vh; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            color: white; padding: 20px; box-sizing: border-box; z-index: 1001; 
            overflow-y: auto; box-shadow: 2px 0 10px rgba(0,0,0,0.2); 
        }
        .sidebar h2 { 
            margin: 0 0 20px 0; font-size: 24px; font-weight: 300; 
            border-bottom: 2px solid rgba(255,255,255,0.3); padding-bottom: 10px; 
        }
        .filter-section { 
            margin-bottom: 20px; background: rgba(255,255,255,0.1); 
            padding: 15px; border-radius: 8px; 
        }
        .skin-type-item { 
            display: flex; align-items: center; margin: 8px 0; padding: 8px; 
            border-radius: 6px; transition: background 0.3s; cursor: pointer; 
            user-select: none; background: rgba(0,0,0,0.2); 
        }
        .skin-type-item.active { background: rgba(255,255,255,0.3); }
        .color-indicator { 
            width: 18px; height: 18px; border-radius: 50%; margin-right: 12px; 
            border: 2px solid white; 
        }
        .slider { 
            width: 100%; -webkit-appearance: none; appearance: none; 
            height: 6px; border-radius: 3px; background: rgba(255,255,255,0.3); 
            outline: none; 
        }
        .slider::-webkit-slider-thumb { 
            -webkit-appearance: none; appearance: none; width: 18px; height: 18px; 
            border-radius: 50%; background: #ffd700; cursor: pointer; 
        }
        .slider-value { 
            font-size: 12px; color: #ffd700; text-align: center; 
            margin-top: 5px; font-weight: bold; 
        }
        .reset-btn { 
            width: 100%; padding: 10px; background: rgba(255,255,255,0.2); 
            color: white; border: none; border-radius: 6px; cursor: pointer; 
            font-size: 14px; margin-top: 10px; 
        }
        .checkbox-group {
            display: flex; flex-direction: column; gap: 8px; margin-top: 10px;
        }
        .checkbox-item {
            display: flex; align-items: center; cursor: pointer;
            padding: 6px 8px; border-radius: 4px; background: rgba(0,0,0,0.2);
            transition: background 0.2s;
        }
        .checkbox-item:hover {
            background: rgba(255,255,255,0.1);
        }
        .checkbox-item input[type="checkbox"] {
            margin-right: 8px; cursor: pointer;
        }
        .checkbox-item label {
            cursor: pointer; font-size: 13px; flex: 1;
        }
        .folium-map { 
            position: absolute; top: 0; left: 320px; right: 0; bottom: 0; z-index: 1000; 
        }
        .leaflet-control-layers { display: none !important; }
        .filter-summary { 
            background: rgba(0,0,0,0.2); padding: 10px; border-radius: 6px; 
            margin-bottom: 15px; font-size: 13px; 
        }
        .filter-summary div:not(:last-child) { margin-bottom: 4px; }
        .race-filter { margin-bottom: 10px; }
        .race-filter label { display: block; margin-bottom: 5px; font-size: 13px; }
    """
    
    type_colors = {'I': '#FFE5E5', 'II': '#FFB3B3', 'III': '#FF8080', 'IV': '#CC6600', 'V': '#8B4513', 'VI': '#654321'}
    skin_type_html = ''.join([
        f'<div class="skin-type-item active" data-type="{skin_type}" onclick="this.classList.toggle(\'active\'); updateFilters();">'
        f'<div class="color-indicator" style="background-color: {color};"></div>'
        f'<span>Type {skin_type}</span></div>'
        for skin_type, color in type_colors.items()
    ])

    race_filter_html = ''.join([
        f'<div class="race-filter">'
        f'<label for="{race_col.lower()}">{data["display_name"]}:</label>'
        f'<input type="range" id="{race_col.lower()}" class="slider" min="0" max="{data["max"]}" value="0" oninput="updateFilters()">'
        f'<div class="slider-value" id="{race_col.lower()}-value">0+</div>'
        f'</div>'
        for race_col, data in race_data.items()
    ])

    status_checkboxes = ''.join([
        f'<div class="checkbox-item">'
        f'<input type="checkbox" id="status-{status.lower()}" checked onchange="updateFilters()">'
        f'<label for="status-{status.lower()}">{status_display_map.get(status, status)}</label>'
        f'</div>'
        for status in all_statuses
    ])

    sidebar_html = f"""
    <div class="sidebar">
        <h2>US Fitzpatrick Trials</h2>
        <div class="filter-summary">
            <div>
                <strong>Studies:</strong> 
                <span id="visible-studies-count">{total_studies}</span> of {total_studies}
            </div>
            <div>
                <strong>Locations:</strong> 
                <span id="visible-locations-count">{total_locations}</span> of {total_locations}
            </div>
        </div>
        <div class="filter-section"><h3>Fitzpatrick Skin Types</h3>{skin_type_html}</div>
        <div class="filter-section">
            <h3>Enrollment</h3>
            <div class="control-group">
                <label for="min-enrollment">Minimum Enrollment:</label>
                <input type="range" id="min-enrollment" class="slider" min="0" max="{max_enrollment}" value="0" oninput="updateFilters()">
                <div class="slider-value" id="min-enrollment-value">0+</div>
            </div>
            <div class="control-group" style="margin-top: 15px;">
                <label style="display: block; margin-bottom: 8px;">Enrollment Type:</label>
                <div class="checkbox-group">
                    <div class="checkbox-item">
                        <input type="checkbox" id="enrollment-actual" checked onchange="updateFilters()">
                        <label for="enrollment-actual">Actual</label>
                    </div>
                    <div class="checkbox-item">
                        <input type="checkbox" id="enrollment-estimated" checked onchange="updateFilters()">
                        <label for="enrollment-estimated">Estimated</label>
                    </div>
                    <div class="checkbox-item">
                        <input type="checkbox" id="enrollment-na" checked onchange="updateFilters()">
                        <label for="enrollment-na">N/A</label>
                    </div>
                </div>
            </div>
        </div>
        <div class="filter-section">
            <h3>Study Status</h3>
            <div class="checkbox-group">
                {status_checkboxes}
            </div>
        </div>
        <div class="filter-section"><h3>Race Demographics</h3>{race_filter_html}</div>
        <div class="filter-section"><button class="reset-btn" onclick="resetAllFilters()">Reset All Filters</button></div>
    </div>
    """

    map_name = m.get_name()
    
    javascript_code = f"""
        let mapInstance;
        let markersLayer = L.layerGroup();
        
        const locationsData = {json.dumps(locations_data)};
        const allRaceColumns = {json.dumps(all_race_columns)};
        const raceDataInfo = {json.dumps(race_data)};
        const totalStudies = {total_studies};
        const totalLocations = {total_locations};
        const maxEnrollment = {max_enrollment};
        const allStatuses = {json.dumps(all_statuses)};
        const statusDisplayMap = {json.dumps(status_display_map)};

        function findMapInstance() {{
            const mapId = document.querySelector('.folium-map').id;
            return window[mapId];
        }}

        window.addEventListener('load', function() {{
            setTimeout(initializeMap, 500);
        }});

        function initializeMap() {{
            mapInstance = findMapInstance();
            if (!mapInstance) {{
                console.error("Map instance not found. Retrying...");
                setTimeout(initializeMap, 500);
                return;
            }}
            
            console.log("Map instance found:", mapInstance);
            markersLayer.addTo(mapInstance);
            updateFilters();
        }}
        
        window.toggleSkinType = function(element) {{
            element.classList.toggle('active');
            updateFilters();
        }}

        function passesFilters(record, enrollmentFilter, enrollmentTypes, statusTypes, raceFilters, activeTypes) {{
            let hasActiveSkinType = false;
            for (const type of activeTypes) {{
                if (record[`Type_${{type}}`] === 1) {{
                    hasActiveSkinType = true;
                    break;
                }}
            }}
            if (!hasActiveSkinType) return false;

            const enrollment = record.enrollment === 'N/A' ? 0 : record.enrollment;
            if (enrollment < enrollmentFilter) return false;

            const enrollmentType = record.enrollment_type || 'N/A';
            if (!enrollmentTypes.includes(enrollmentType.toUpperCase())) return false;

            const status = record.status || 'N/A';
            if (!statusTypes.includes(status)) return false;

            for (const [raceCol, minVal] of Object.entries(raceFilters)) {{
                if ((record[raceCol] || 0) < minVal) return false;
            }}
            return true;
        }}

        window.updateFilters = function() {{
            if (!mapInstance) {{
                console.warn('Map not ready for update.');
                return;
            }}

            const activeTypes = Array.from(document.querySelectorAll('.skin-type-item.active')).map(el => el.dataset.type);
            const enrollmentFilter = parseInt(document.getElementById('min-enrollment').value);
            document.getElementById('min-enrollment-value').textContent = enrollmentFilter + '+';
            const enrollmentTypes = [];
            if (document.getElementById('enrollment-actual').checked) enrollmentTypes.push('ACTUAL');
            if (document.getElementById('enrollment-estimated').checked) enrollmentTypes.push('ESTIMATED');
            if (document.getElementById('enrollment-na').checked) enrollmentTypes.push('N/A');
            const statusTypes = [];
            allStatuses.forEach(status => {{
                const checkbox = document.getElementById(`status-${{status.toLowerCase()}}`);
                if (checkbox && checkbox.checked) {{
                    statusTypes.push(status);
                }}
            }});
            const raceFilters = {{}};
            for (const raceCol in raceDataInfo) {{
                const elId = raceCol.toLowerCase();
                const element = document.getElementById(elId);
                if (element) {{
                    const minValue = parseInt(element.value);
                    raceFilters[raceCol] = minValue;
                    document.getElementById(elId + '-value').textContent = minValue + '+';
                }}
            }}

            markersLayer.clearLayers();
            let visibleLocations = 0;
            const visibleStudies = new Set();

            for (const [locKey, studiesAtLoc] of Object.entries(locationsData)) {{
                const passingStudies = studiesAtLoc.filter(study => 
                    passesFilters(study, enrollmentFilter, enrollmentTypes, statusTypes, raceFilters, activeTypes)
                );

                if (passingStudies.length > 0) {{
                    visibleLocations++;
                    passingStudies.forEach(study => visibleStudies.add(study.nctId));
                    
                    const [lat, lon] = locKey.split(',').map(Number);
                    
                    let popupHtml = '<div style="font-family: Arial, sans-serif; max-height: 300px; overflow-y: auto; min-width: 350px;">';
                    passingStudies.forEach((study, i) => {{
                        let raceHtml = "", totalParticipants = 0;
                        allRaceColumns.forEach(raceCol => {{
                            const count = study[raceCol] || 0;
                            if (count > 0) {{ totalParticipants += count; raceHtml += `<li>${{raceDataInfo[raceCol]?.display_name || raceCol}}: <strong>${{count}}</strong></li>`; }}
                        }});
                        if (raceHtml) raceHtml = `<p style="margin:5px 0 3px;"><strong>Demographics:</strong></p><ul style="margin:0;padding-left:20px;">${{raceHtml}}</ul>`;
                        
                        const enrollmentDisplay = study.enrollment !== 'N/A' && study.enrollment_type !== 'N/A' 
                            ? `${{study.enrollment}} (${{study.enrollment_type}})`
                            : (study.enrollment || 'N/A');
                        const statusDisplay = statusDisplayMap[study.status] || study.status;

                        const includedSkinTypes = [];
                        const typeRomans = ['I', 'II', 'III', 'IV', 'V', 'VI'];
                        typeRomans.forEach(roman => {{
                            if (study[`Type_${{roman}}`] === 1) {{
                                includedSkinTypes.push(roman);
                            }}
                        }});
                        const skinTypeDisplay = includedSkinTypes.length > 0 ? includedSkinTypes.join(', ') : 'Not Specified';

                        popupHtml += `<div style="border-top: ${{i > 0 ? '1px solid #ccc' : 'none'}}; padding: 10px 5px;">
                                        <h4 style="margin:0 0 10px 0;">Study Details</h4>
                                        <p><strong>NCT ID:</strong> <a href="https://clinicaltrials.gov/study/${{study.nctId}}" target="_blank">${{study.nctId}}</a></p>
                                        <p><strong>Status:</strong> ${{statusDisplay}}</p>
                                        <p><strong>Enrollment:</strong> <strong>${{enrollmentDisplay}}</strong></p>
                                        <p><strong>Facility:</strong> ${{study.facility}}</p>
                                        <p><strong>Skin Types:</strong> ${{skinTypeDisplay}}</p>
                                        ${{raceHtml}}
                                     </div>`;
                    }});
                    popupHtml += '</div>';
                    
                    const markerRadius = 6 + Math.sqrt(passingStudies.length);
                    
                    L.circleMarker([lat, lon], {{
                        radius: markerRadius,
                        color: '#ffffff',
                        weight: 2,
                        fillColor: '#764ba2',
                        fillOpacity: 0.8
                    }})
                    .bindPopup(popupHtml, {{maxWidth: 400}})
                    .bindTooltip(`${{passingStudies[0].city}} (${{passingStudies.length}} studies)`)
                    .addTo(markersLayer);
                }}
            }}
            document.getElementById('visible-locations-count').textContent = visibleLocations;
            document.getElementById('visible-studies-count').textContent = visibleStudies.size;
        }};

        window.resetAllFilters = function() {{
            document.querySelectorAll('.skin-type-item').forEach(item => item.classList.add('active'));
            document.querySelectorAll('.slider').forEach(slider => slider.value = 0);
            document.getElementById('enrollment-actual').checked = true;
            document.getElementById('enrollment-estimated').checked = true;
            document.getElementById('enrollment-na').checked = true;
            allStatuses.forEach(status => {{
                const checkbox = document.getElementById(`status-${{status.toLowerCase()}}`);
                if (checkbox) checkbox.checked = true;
            }});
            updateFilters();
        }};
    """
    
    m.get_root().header.add_child(Element(f"<style>{css_rules}</style>"))
    m.get_root().html.add_child(Element(sidebar_html))
    m.get_root().script.add_child(Element(javascript_code))

    m.save(filename)
    print(f"\n[*] Success! Interactive map saved to '{filename}'.")

def main():
    """Main function to run the entire data processing and mapping pipeline."""
    fetch_clinical_trials_data(API_BASE_URL, SEARCH_KEYWORD, RAW_JSON_FILENAME)
    try:
        with open(RAW_JSON_FILENAME, 'r', encoding='utf-8') as f: 
            studies = json.load(f).get('studies', [])
        print(f"[*] Loaded {len(studies)} studies from '{RAW_JSON_FILENAME}'.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[!] Error loading raw JSON file: {e}. Please run the script again.")
        return

    all_facility_rows = []
    all_race_keys = set(FILTERS.get('min_participants_by_race', {}).keys())

    for study in studies:
        nct_id = study.get('protocolSection', {}).get('identificationModule', {}).get('nctId', 'N/A')
        details = extract_study_details(study, COUNTRY_TO_ISOLATE)
        if not details['us_facilities']: continue
        
        for key in details['race_data']:
            all_race_keys.add(key)
        
        inclusion_sentences = [s['sentence'] for s in parse_eligibility_criteria(study, SEARCH_KEYWORD) if not s['is_exclusion']]
        if not inclusion_sentences: continue
        
        score_data = extract_and_standardize_scores(inclusion_sentences[0])
        if score_data['extracted_score'] == 'Not a Skin Type Score': continue
            
        for facility in details['us_facilities']:
            row = {'nctId': nct_id, 'status': details['status'], 'enrollment': details['enrollment'], 'enrollment_type': details['enrollment_type']}
            row.update(score_data)
            row.update(facility)
            row.update(details['race_data'])
            all_facility_rows.append(row)
    
    if not all_facility_rows:
        print("[!] No US-based studies with specified criteria and geo-coordinates found.")
        return

    df = pd.DataFrame(all_facility_rows)
    for race_col in all_race_keys:
        if race_col not in df.columns: df[race_col] = 0
    df['enrollment'] = df['enrollment'].replace(0, 'N/A')
    df.fillna({'enrollment': 'N/A'}, inplace=True)
    df.fillna(0, inplace=True)

    print(f"[*] Processed data into {len(df)} facility-level records.")

    print("\n[*] Identifying studies that passed initial parsing but have no specific score assigned...")
    skin_type_cols = [f'Type_{r}' for r in ['I', 'II', 'III', 'IV', 'V', 'VI']]
    existing_skin_type_cols = [col for col in skin_type_cols if col in df.columns]
    
    unparsed_mask = df[existing_skin_type_cols].sum(axis=1) == 0
    unparsed_df = df[unparsed_mask]

    if not unparsed_df.empty:
        num_unparsed_studies = unparsed_df['nctId'].nunique()
        print(f"✅ Found {len(unparsed_df)} records from {num_unparsed_studies} unique studies with no specific Fitzpatrick Type flags.")
        
        output_cols = ['nctId', 'facility', 'city', 'state', 'extracted_score']
        final_output_cols = [col for col in output_cols if col in unparsed_df.columns]
        
        unparsed_df[final_output_cols].to_csv('usa_map/unparsed_studies.csv', index=False)
        print(f"[*] This list has been saved to 'unparsed_studies.csv' for your review.")
    else:
        print("[*] All processed studies have at least one specific Fitzpatrick Type flag assigned.")
    
    if not unparsed_df.empty:
        print(f"\n[*] Dropping {len(unparsed_df)} records with no specific scores from the main dataset.")
        df = df[~unparsed_mask] # Invert the mask to keep only the rows WITH scores
        print(f"[*] {len(df)} records remaining for the final CSV and map.")

    if df.empty:
        print("[!] No records to process after parsing. All studies were unparsed.")
        return

    try:
        df.to_csv(FINAL_OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"[*] Success! Final dataset saved to '{FINAL_OUTPUT_CSV}'.")
    except IOError as e: 
        print(f"[!] Error writing final CSV file: {e}")

    create_interactive_map_with_sidebar(df.to_dict('records'), MAP_OUTPUT_HTML)
if __name__ == "__main__":
    main()