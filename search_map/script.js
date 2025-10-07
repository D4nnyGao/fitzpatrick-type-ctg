document.addEventListener('DOMContentLoaded', () => {
    // --- Global Variables ---
    let mapInstance, markersLayer, heatmapLayer;
    let allLocationsData = {}, allHeatmapData = [];
    let allStatuses = [], allPhases = [], minYear, maxYear, maxEnrollment;
    
    // --- DOM Element References ---
    const searchForm = document.getElementById('search-form');
    const loader = document.getElementById('loader');
    const resultsSummary = document.getElementById('results-summary');
    const filtersContainer = document.getElementById('filters-container');

    // --- Initialize Base Map ---
    mapInstance = L.map('map', { center: [39.8283, -98.5795], zoom: 4 });
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    }).addTo(mapInstance);

    // --- Attach Event Listeners ---
    searchForm.addEventListener('submit', handleSearch);

    async function handleSearch(e) {
        e.preventDefault();
        loader.classList.remove('hidden');
        filtersContainer.classList.add('hidden');
        resultsSummary.classList.add('hidden');
        if (markersLayer) mapInstance.removeLayer(markersLayer);
        markersLayer = L.layerGroup();
        if (heatmapLayer) heatmapLayer.setLatLngs([]);

        try {
            const rawStudies = await fetchAllTrialData();
            if (rawStudies.length === 0) {
                resultsSummary.textContent = 'No studies found matching your criteria.';
                resultsSummary.classList.remove('hidden');
                return;
            }
            processApiData(rawStudies);
            buildDynamicFilterUI();
            initializeMapLayersAndFilters();

        } catch (error) {
            console.error("Search failed:", error);
            resultsSummary.textContent = `An error occurred: ${error.message}`;
        } finally {
            loader.classList.add('hidden');
        }
    }

    async function fetchAllTrialData() {
        const keywords = document.getElementById('keywords').value;
        const searchArea = document.getElementById('search-area').value;
        const country = document.getElementById('country').value;
        const allStudies = [];
        let nextPageToken = null;
        
        const areaMap = { 'eligibility': 'EligibilityCriteria', 'title': 'OfficialTitle', 'summary': 'BriefSummary' };
        const searchTerm = searchArea in areaMap ? `AREA[${areaMap[searchArea]}](${keywords})` : `(${keywords})`;
        const fullQuery = `${searchTerm} AND SEARCH[Location](AREA[LocationCountry]"${country}")`;

        do {
            const params = new URLSearchParams({
                'query.term': fullQuery,
                'fields': "NCTId,protocolSection",
                'pageSize': 1000
            });
            if (nextPageToken) params.set('pageToken', nextPageToken);
            
            const response = await fetch(`https://clinicaltrials.gov/api/v2/studies?${params.toString()}`);
            if (!response.ok) throw new Error(`API request failed with status ${response.status}`);
            
            const data = await response.json();
            if (data.studies) allStudies.push(...data.studies);
            nextPageToken = data.nextPageToken;
        } while (nextPageToken);

        return allStudies;
    }

    function processApiData(studies) {
        const locations = {};
        const yearValues = [], enrollmentValues = new Set(), statusValues = new Set(), phaseValues = new Set();
        const country = document.getElementById('country').value;

        studies.forEach(study => {
            const proto = study.protocolSection;
            if (!proto) return;
            const locationsList = proto.contactsLocationsModule?.locations || [];
            locationsList.forEach(loc => {
                if (loc.country === country && loc.geoPoint) {
                    const key = `${loc.geoPoint.lat.toFixed(6)},${loc.geoPoint.lon.toFixed(6)}`;
                    if (!locations[key]) locations[key] = [];
                    const studyDetails = {
                        nctId: proto.identificationModule.nctId,
                        status: proto.statusModule.overallStatus,
                        last_update_year: proto.statusModule.lastUpdatePostDateStruct.date.substring(0, 4),
                        enrollment: proto.designModule.enrollmentInfo?.count ?? 'N/A',
                        enrollment_type: proto.designModule.enrollmentInfo?.type ?? 'N/A',
                        phase: proto.designModule.phases?.join(', ') || 'N/A',
                        central_contacts: proto.contactsLocationsModule?.centralContacts || [],
                        overall_officials: proto.contactsLocationsModule?.overallOfficials || [],
                        facility: loc.facility, city: loc.city,
                    };
                    locations[key].push(studyDetails);
                    if (/\d{4}/.test(studyDetails.last_update_year)) yearValues.push(parseInt(studyDetails.last_update_year));
                    if (typeof studyDetails.enrollment === 'number') enrollmentValues.add(studyDetails.enrollment);
                    statusValues.add(studyDetails.status);
                    phaseValues.add(studyDetails.phase);
                }
            });
        });
        allLocationsData = locations;
        allHeatmapData = Object.entries(locations).map(([key, studies]) => {
            const [lat, lon] = key.split(',').map(Number);
            return [lat, lon, Math.log1p(studies.length)];
        });
        allStatuses = Array.from(statusValues).sort();
        allPhases = Array.from(phaseValues).sort();
        minYear = yearValues.length > 0 ? Math.min(...yearValues) : 2000;
        maxYear = yearValues.length > 0 ? Math.max(...yearValues) : new Date().getFullYear();
        maxEnrollment = enrollmentValues.size > 0 ? Math.max(...Array.from(enrollmentValues)) : 1000;
    }

    function buildDynamicFilterUI() {
        const statusDisplayMap = {"ACTIVE_NOT_RECRUITING": "Active, not recruiting", "COMPLETED": "Completed", "ENROLLING_BY_INVITATION": "Enrolling by invitation", "NOT_YET_RECRUITING": "Not yet recruiting", "RECRUITING": "Recruiting", "SUSPENDED": "Suspended", "TERMINATED": "Terminated", "WITHDRAWN": "Withdrawn", "UNKNOWN": "Unknown status"};
        document.getElementById('status-group').innerHTML = allStatuses.map(s => `<div class="checkbox-item"><input type="checkbox" id="status-${s.toLowerCase()}" data-status="${s}" checked onchange="updateFilters()"><label for="status-${s.toLowerCase()}">${statusDisplayMap[s] || s}</label></div>`).join('');
        const phaseDisplayMap = {'NA': 'Not Applicable', 'EARLY_PHASE1': 'Early Phase 1', 'PHASE1': 'Phase 1', 'PHASE2': 'Phase 2', 'PHASE3': 'Phase 3', 'PHASE4': 'Phase 4', 'N/A': 'N/A'};
        document.getElementById('phase-group').innerHTML = allPhases.map(p => `<div class="checkbox-item"><input type="checkbox" id="phase-${p.toLowerCase().replace(/[\s,]+/g, '-')}" data-phase="${p}" checked onchange="updateFilters()"><label for="phase-${p.toLowerCase().replace(/[\s,]+/g, '-')}">${phaseDisplayMap[p] || p}</label></div>`).join('');
        document.getElementById('enrollment-type-group').innerHTML = `<div class="checkbox-item"><input type="checkbox" id="enrollment-actual" checked onchange="updateFilters()"><label for="enrollment-actual">Actual</label></div><div class="checkbox-item"><input type="checkbox" id="enrollment-estimated" checked onchange="updateFilters()"><label for="enrollment-estimated">Estimated</label></div><div class="checkbox-item"><input type="checkbox" id="enrollment-na" checked onchange="updateFilters()"><label for="enrollment-na">N/A</label></div>`;
        const enrollmentSlider = document.getElementById('min-enrollment');
        enrollmentSlider.max = maxEnrollment;
        enrollmentSlider.value = 0;
        document.getElementById('min-enrollment-value').textContent = '0+';
        const yearSlider = document.getElementById('year-range');
        yearSlider.min = minYear;
        yearSlider.max = maxYear;
        yearSlider.value = minYear;
        document.getElementById('year-range-value').textContent = `${minYear}+`;
        filtersContainer.classList.remove('hidden');
    }

    function initializeMapLayersAndFilters() {
        if (!markersLayer) markersLayer = L.layerGroup();
        if (!heatmapLayer) {
             heatmapLayer = L.heatLayer(allHeatmapData, { 
                radius: 25, blur: 15, 
                gradient: {0.4:'blue', 0.6:'lime', 0.8:'yellow', 1.0:'red'}
            });
        } else {
             heatmapLayer.setLatLngs(allHeatmapData);
        }
        updateVisualization();
        updateFilters();
    }
    
    function buildContactsHtml(study) {
        const { central_contacts, overall_officials } = study;
        if (!central_contacts?.length && !overall_officials?.length) return '';
        let officialsHtml = '';
        if (overall_officials?.length) {
            officialsHtml = '<h4>Officials</h4><ul>' + overall_officials.map(c => `<li><strong>${c.name || 'N/A'}</strong> (${(c.role || '').replace(/_/g, ' ')}), <em>${c.affiliation || 'N/A'}</em></li>`).join('') + '</ul>';
        }
        let contactsHtml = '';
        if (central_contacts?.length) {
            contactsHtml = '<h4>Central Contacts</h4><ul>' + central_contacts.map(c => {
                let contactInfo = c.email ? `<a href="mailto:${c.email}">${c.email}</a>` : '';
                if (c.phone) contactInfo += (contactInfo ? ` / ${c.phone}` : c.phone);
                return `<li><strong>${c.name || 'N/A'}</strong> (${c.role}): ${contactInfo || 'No contact info'}</li>`;
            }).join('') + '</ul>';
        }
        return `<details class="popup-details"><summary>Contacts & Officials</summary><div class="popup-details-content">${officialsHtml}${contactsHtml}</div></details>`;
    }

    window.updateVisualization = function() {
        const vizType = document.querySelector('input[name="viz-type"]:checked').value;
        if (vizType === 'dots') {
            if (mapInstance.hasLayer(heatmapLayer)) mapInstance.removeLayer(heatmapLayer);
            if (!mapInstance.hasLayer(markersLayer)) mapInstance.addLayer(markersLayer);
        } else {
            if (mapInstance.hasLayer(markersLayer)) mapInstance.removeLayer(markersLayer);
            if (!mapInstance.hasLayer(heatmapLayer)) mapInstance.addLayer(heatmapLayer);
        }
    };

    function passesFilters(record, enrollmentFilter, enrollmentTypes, statusTypes, yearFilter, activePhases) {
        if (!activePhases.includes(record.phase || 'N/A')) return false;
        if (!statusTypes.includes(record.status || 'N/A')) return false;
        const recordYear = parseInt(record.last_update_year);
        if (!isNaN(recordYear) && recordYear < yearFilter) return false;
        const enrollment = record.enrollment === 'N/A' ? 0 : record.enrollment;
        if (enrollment < enrollmentFilter) return false;
        if (!enrollmentTypes.includes((record.enrollment_type || 'N/A').toUpperCase())) return false;
        return true;
    }

    window.updateFilters = function() {
        if (!mapInstance || !markersLayer) return;
        const activePhases = Array.from(document.querySelectorAll('#phase-group input:checked')).map(el => el.dataset.phase);
        const yearFilter = parseInt(document.getElementById('year-range').value);
        document.getElementById('year-range-value').textContent = yearFilter + '+';
        const enrollmentFilter = parseInt(document.getElementById('min-enrollment').value);
        document.getElementById('min-enrollment-value').textContent = enrollmentFilter + '+';
        const statusTypes = Array.from(document.querySelectorAll('#status-group input:checked')).map(el => el.dataset.status);
        const enrollmentTypes = [];
        if (document.getElementById('enrollment-actual').checked) enrollmentTypes.push('ACTUAL');
        if (document.getElementById('enrollment-estimated').checked) enrollmentTypes.push('ESTIMATED');
        if (document.getElementById('enrollment-na').checked) enrollmentTypes.push('N/A');
        markersLayer.clearLayers();
        const newHeatmapData = [];
        let visibleLocations = 0;
        const visibleStudies = new Set();
        const totalStudiesInView = new Set(Object.values(allLocationsData).flat().map(s => s.nctId));
        for (const [locKey, studiesAtLoc] of Object.entries(allLocationsData)) {
            const passingStudies = studiesAtLoc.filter(study => passesFilters(study, enrollmentFilter, enrollmentTypes, statusTypes, yearFilter, activePhases));
            if (passingStudies.length > 0) {
                visibleLocations++;
                passingStudies.forEach(study => visibleStudies.add(study.nctId));
                const [lat, lon] = locKey.split(',').map(Number);
                
                let popupHtml = `<div>`;
                passingStudies.forEach((study, i) => {
                    const enrollmentTypeStr = study.enrollment_type.charAt(0).toUpperCase() + study.enrollment_type.slice(1).toLowerCase();
                    const enrollmentDisplay = (typeof study.enrollment === 'number' && study.enrollment_type !== 'N/A') ? `<strong>${study.enrollment}</strong> (${enrollmentTypeStr})` : (study.enrollment || 'N/A');
                    const contactsHtml = buildContactsHtml(study);
                    
                    // --- MODIFIED: Uses CSS class for styling instead of inline styles ---
                    popupHtml += `<div class="popup-study-container">
                        <h4>Study <a href="https://clinicaltrials.gov/study/${study.nctId}" target="_blank">${study.nctId}</a></h4>
                        <p><strong>Facility:</strong> ${study.facility || 'N/A'}</p>
                        <p><strong>Status:</strong> ${study.status}</p>
                        <p><strong>Phase:</strong> ${study.phase}</p>
                        <p><strong>Enrollment:</strong> ${enrollmentDisplay}</p>
                        <p><strong>Last Updated:</strong> ${study.last_update_year}</p>
                        ${contactsHtml}
                    </div>`;
                });
                popupHtml += '</div>';

                const radius = Math.min(6 + Math.log2(passingStudies.length) * 2.5, 20);
                L.circleMarker([lat, lon], { radius: radius, color: '#ffffff', weight: 2, fillColor: '#764ba2', fillOpacity: 0.8 })
                    .bindPopup(popupHtml, {maxWidth: 320, maxHeight: 280})
                    .bindTooltip(`${passingStudies[0].city || 'Location'} (${passingStudies.length} studies)`)
                    .addTo(markersLayer);
                newHeatmapData.push([lat, lon, Math.log1p(passingStudies.length)]);
            }
        }
        heatmapLayer.setLatLngs(newHeatmapData);
        resultsSummary.innerHTML = `<div><strong>Showing:</strong> <span id="visible-studies-count">${visibleStudies.size}</span> of ${totalStudiesInView.size} studies</div><div><strong>At:</strong> <span id="visible-locations-count">${visibleLocations}</span> of ${Object.keys(allLocationsData).length} locations</div>`;
        resultsSummary.classList.remove('hidden');
    };

    window.resetAllFilters = function() {
        if (!filtersContainer.classList.contains('hidden')) {
            document.getElementById('viz-dots').checked = true;
            updateVisualization();
            document.querySelectorAll('.slider').forEach(slider => { slider.value = slider.min; });
            document.querySelectorAll('#filters-container input[type=checkbox]').forEach(cb => cb.checked = true);
            updateFilters();
        }
    };
});