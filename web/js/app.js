/**
 * Camunda Backup Controller - Web UI
 * Single-page dashboard application
 */

'use strict';

// ============================================================
// Constants
// ============================================================
const BACKUP_POLLING_INTERVAL_MS = 5000;
const MODAL_TRANSITION_MS = 200;
const TOAST_REMOVAL_ANIMATION_MS = 300;
const TOAST_DURATION_MS = 5000;

/**
 * Canonical list of Camunda component names with endpoint support.
 * Each entry maps to a form field name: `${name}_backup_endpoint`.
 * The status endpoint is derived automatically as backup endpoint + /{backupId}.
 */
const CAMUNDA_COMPONENTS = ['zeebe', 'operate', 'tasklist', 'optimize'];

/**
 * All valid endpoint field names for a Camunda instance, derived from CAMUNDA_COMPONENTS.
 * Used to safely read/write component endpoint fields without dynamic string construction.
 */
const COMPONENT_ENDPOINT_FIELDS = CAMUNDA_COMPONENTS.map(name => `${name}_backup_endpoint`);

// ============================================================
// API Client
// ============================================================
const api = {
    async request(method, path, body) {
        const opts = {
            method,
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
            },
        };
        if (body) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(body);
        }
        try {
            const res = await fetch(path, opts);
            const contentType = res.headers.get('Content-Type') || '';
            if (contentType.includes('text/plain')) {
                const text = await res.text();
                if (!res.ok) throw { status: res.status, message: text };
                return text;
            }
            const data = contentType.includes('application/json') ? await res.json() : null;
            if (!res.ok) {
                const msg = data?.message || `Request failed (${res.status})`;
                throw { status: res.status, message: msg };
            }
            return data;
        } catch (err) {
            if (err.status) throw err;
            throw { status: 0, message: err.message || 'Network error' };
        }
    },
    get(path) { return this.request('GET', path); },
    post(path, body) { return this.request('POST', path, body); },
    put(path, body) { return this.request('PUT', path, body); },
    del(path) { return this.request('DELETE', path); },
};

// ============================================================
// State
// ============================================================
const state = {
    currentTab: 'dashboard',
    instances: [],
    selectedInstanceId: null,
    backupFilter: 'all',
    pollingIntervalId: null,
    activeBackupInstanceId: null,
    activeBackupId: null,
    systemStatus: null,
    pollingPaused: false,
    instancesStale: true,
};

// ============================================================
// Initialization
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
    initTabs();
    initModalListeners();
    showTab('dashboard');
});

// ============================================================
// Tab Router
// ============================================================
function initTabs() {
    document.querySelectorAll('[data-tab]').forEach(btn => {
        btn.addEventListener('click', () => showTab(btn.dataset.tab));
    });
}

function showTab(name) {
    state.currentTab = name;

    // Update tab button states
    document.querySelectorAll('[data-tab]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === name);
    });

    // Show/hide tab content
    document.querySelectorAll('[data-tab-content]').forEach(panel => {
        panel.classList.toggle('hidden', panel.dataset.tabContent !== name);
    });

    // Pause polling when leaving dashboard, resume when returning
    if (name !== 'dashboard') {
        pauseBackupPolling();
    } else if (state.pollingPaused) {
        resumeBackupPolling();
    }

    // Load data for the activated tab
    switch (name) {
        case 'dashboard':
            loadDashboard();
            break;
        case 'instances':
            loadInstances();
            break;
        case 'backups':
            loadBackupsTab();
            break;
    }
}

// ============================================================
// Dashboard
// ============================================================
async function loadDashboard() {
    const statusEl = document.getElementById('dashboard-content');
    if (!statusEl) return;

    try {
        const data = await api.get('api/status');
        state.systemStatus = data;
        renderDashboard(data);
        updateHeaderStatus(data);

        if (data.active_backups > 0) {
            startBackupPolling();
        } else {
            stopBackupPolling();
        }
    } catch (err) {
        statusEl.innerHTML = `<div class="empty-state"><p>Failed to load system status</p><p class="text-sm mt-1">${err.message}</p></div>`;
        updateHeaderStatus(null);
    }
}

function renderDashboard(data) {
    const el = document.getElementById('dashboard-content');
    if (!el) return;

    el.innerHTML = `
        <!-- System Health & Instance Summary -->
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
            <!-- Scheduler Card -->
            <div class="bg-white rounded-lg border border-gray-200 p-5 card-hover">
                <div class="flex items-center justify-between mb-3">
                    <h3 class="text-sm font-semibold text-gray-500 uppercase tracking-wide">Scheduler</h3>
                    <span class="status-dot ${data.scheduler.running ? 'status-dot-green' : 'status-dot-red'}"></span>
                </div>
                <p class="text-2xl font-bold text-gray-900">${data.scheduler.running ? 'Running' : 'Stopped'}</p>
                <div class="mt-2 text-sm text-gray-500">
                    <span>${data.scheduler.jobs_count} total jobs</span> · 
                    <span>${data.scheduler.enabled_jobs} enabled</span>
                </div>
            </div>

            <!-- Storage Card -->
            <div class="bg-white rounded-lg border border-gray-200 p-5 card-hover">
                <div class="flex items-center justify-between mb-3">
                    <h3 class="text-sm font-semibold text-gray-500 uppercase tracking-wide">Storage</h3>
                    <span class="status-dot ${data.storage.file_storage_healthy && data.storage.s3_storage_healthy ? 'status-dot-green' : 'status-dot-red'}"></span>
                </div>
                <div class="space-y-2">
                    <div class="flex items-center gap-2">
                        <span class="status-dot ${data.storage.file_storage_healthy ? 'status-dot-green' : 'status-dot-red'}"></span>
                        <span class="text-sm text-gray-700">File Storage</span>
                    </div>
                    <div class="flex items-center gap-2">
                        <span class="status-dot ${data.storage.s3_storage_healthy ? 'status-dot-green' : 'status-dot-red'}"></span>
                        <span class="text-sm text-gray-700">S3 Storage</span>
                    </div>
                </div>
            </div>

            <!-- Instances Card -->
            <div class="bg-white rounded-lg border border-gray-200 p-5 card-hover cursor-pointer" onclick="showTab('instances')">
                <div class="flex items-center justify-between mb-3">
                    <h3 class="text-sm font-semibold text-gray-500 uppercase tracking-wide">Instances</h3>
                    <svg class="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
                    </svg>
                </div>
                <p class="text-2xl font-bold text-gray-900">${data.camunda_instances.total}</p>
                <div class="mt-2 text-sm text-gray-500">
                    <span class="text-green-600 font-medium">${data.camunda_instances.enabled} enabled</span> · 
                    <span class="text-red-600 font-medium">${data.camunda_instances.disabled} disabled</span>
                </div>
            </div>
        </div>

        <!-- Active Backup Panel -->
        <div id="active-backup-panel" class="${data.active_backups > 0 ? '' : 'hidden'}">
            <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-5 mb-6">
                <div class="flex items-center gap-2 mb-2">
                    <div class="spinner"></div>
                    <h3 class="text-sm font-semibold text-yellow-800 uppercase tracking-wide">Backup In Progress</h3>
                </div>
                <div id="active-backup-details" class="text-sm text-yellow-700">
                    <p>A backup is currently running. Details will appear here when available.</p>
                </div>
            </div>
        </div>

        <!-- Quick Actions -->
        <div class="bg-white rounded-lg border border-gray-200 p-5">
            <h3 class="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">Quick Actions</h3>
            <div id="quick-actions" class="flex flex-wrap gap-2">
                <p class="text-sm text-gray-500">Loading instances...</p>
            </div>
        </div>
    `;

    // Load quick action buttons
    loadQuickActions();
}

async function loadQuickActions() {
    const container = document.getElementById('quick-actions');
    if (!container) return;

    try {
        const instances = await getInstances();

        if (!instances || instances.length === 0) {
            container.innerHTML = '<p class="text-sm text-gray-500">No instances configured. <a href="#" onclick="showTab(\'instances\')" class="text-blue-600 hover:underline">Add one</a>.</p>';
            return;
        }

        container.innerHTML = instances.map(instance => `
            <button onclick="triggerBackup('${escapeForInlineHandler(instance.id)}', '${escapeForInlineHandler(instance.name)}')"
                class="inline-flex items-center gap-1.5 px-3 py-1.5 ${instance.enabled ? 'bg-blue-600 hover:bg-blue-700' : 'bg-gray-500 hover:bg-gray-600'} text-white text-sm font-medium rounded-md transition-colors">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"/>
                </svg>
                Backup ${escapeHtml(instance.name)}${!instance.enabled ? ' <span class="opacity-75 text-xs">(unscheduled)</span>' : ''}
            </button>
        `).join('');
    } catch (err) {
        container.innerHTML = `<p class="text-sm text-red-500">Failed to load instances</p>`;
    }
}

function updateHeaderStatus(data) {
    const dot = document.getElementById('header-status-dot');
    if (!dot) return;

    dot.className = 'status-dot';
    if (!data) {
        dot.classList.add('status-dot-red');
    } else if (data.active_backups > 0) {
        dot.classList.add('status-dot-yellow');
    } else {
        dot.classList.add('status-dot-green');
    }
}

// ============================================================
// Backup Polling
// ============================================================
function startBackupPolling() {
    if (state.pollingIntervalId) return;
    state.pollingIntervalId = setInterval(async () => {
        try {
            const data = await api.get('api/status');
            state.systemStatus = data;
            updateHeaderStatus(data);

            if (data.active_backups === 0) {
                stopBackupPolling();
                await notifyBackupResult();
                if (state.currentTab === 'dashboard') loadDashboard();
            }
        } catch (err) { console.error('Backup polling error:', err.message || err); }
    }, BACKUP_POLLING_INTERVAL_MS);
}

/**
 * Reports the outcome of the just-finished backup. Looks up the actual final
 * status of the tracked backup so we surface failures/incompletes instead of
 * always claiming success.
 */
async function notifyBackupResult() {
    const instanceId = state.activeBackupInstanceId;
    const backupId = state.activeBackupId;
    // Clear tracking up front so we never double-report the same backup.
    state.activeBackupInstanceId = null;
    state.activeBackupId = null;

    if (!instanceId || !backupId) {
        showToast('Backup finished', 'info');
        return;
    }

    try {
        const backup = await api.get(`api/camundas/${instanceId}/backups/${backupId}`);
        const status = (backup.status || '').toUpperCase();
        if (status === 'COMPLETED') {
            showToast('Backup completed successfully', 'success');
        } else if (status === 'FAILED') {
            showToast(`Backup failed${backup.error_message ? ': ' + backup.error_message : ''}`, 'error');
        } else if (status === 'INCOMPLETE') {
            showToast('Backup incomplete: not all components finished', 'warning');
        } else {
            showToast(`Backup finished with status: ${backup.status || 'unknown'}`, 'info');
        }
    } catch (err) {
        showToast('Backup finished, but its final status could not be determined', 'warning');
    }
}

function stopBackupPolling() {
    if (state.pollingIntervalId) {
        clearInterval(state.pollingIntervalId);
        state.pollingIntervalId = null;
    }
    state.pollingPaused = false;
}

function pauseBackupPolling() {
    if (state.pollingIntervalId) {
        clearInterval(state.pollingIntervalId);
        state.pollingIntervalId = null;
        state.pollingPaused = true;
    }
}

function resumeBackupPolling() {
    state.pollingPaused = false;
    startBackupPolling();
}

// ============================================================
// Trigger Backup
// ============================================================
async function triggerBackup(instanceId, instanceName) {
    try {
        const data = await api.post(`api/camundas/${instanceId}/backup`);
        state.activeBackupInstanceId = instanceId;
        state.activeBackupId = data.backup_id;
        showToast(`Backup triggered for ${instanceName} (ID: ${data.backup_id})`, 'success');
        startBackupPolling();
        showTab('dashboard');
    } catch (err) {
        showToast(err.message || 'Failed to trigger backup', 'error');
    }
}

// ============================================================
// Instances Tab
// ============================================================
async function loadInstances() {
    const el = document.getElementById('instances-content');
    if (!el) return;

    el.innerHTML = '<div class="flex justify-center py-8"><div class="spinner"></div></div>';

    try {
        const instances = await api.get('api/camundas');
        state.instances = instances || [];
        state.instancesStale = false;
        renderInstancesTable(instances);
    } catch (err) {
        el.innerHTML = `<div class="empty-state"><p>Failed to load instances</p><p class="text-sm mt-1">${err.message}</p></div>`;
    }
}

/** Mark instances stale so the next tab load re-fetches. */
function invalidateInstancesCache() {
    state.instancesStale = true;
}

/** Return cached instances or fetch fresh if stale/empty. */
async function getInstances() {
    if (!state.instancesStale && state.instances.length > 0) {
        return state.instances;
    }
    const instances = await api.get('api/camundas');
    state.instances = instances || [];
    state.instancesStale = false;
    return state.instances;
}

function renderInstancesTable(instances) {
    const el = document.getElementById('instances-content');
    if (!el) return;

    if (!instances || instances.length === 0) {
        el.innerHTML = `
            <div class="empty-state">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>
                <p class="text-lg font-medium text-gray-600">No Camunda Instances</p>
                <p class="text-sm mt-1">Get started by adding your first instance.</p>
                <button onclick="openInstanceForm()" class="mt-4 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700">
                    Add Instance
                </button>
            </div>
        `;
        return;
    }

    el.innerHTML = `
        <div class="overflow-x-auto">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>URL</th>
                        <th>Connectivity</th>
                        <th>Schedule</th>
                        <th>Cron</th>
                        <th class="hidden lg:table-cell">Last Backup</th>
                        <th class="hidden lg:table-cell">Last Status</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${instances.map(instance => {
                        const esComp = (instance.components || []).find(c => c.name === 'elasticsearch');
                        const esEnabled = esComp && esComp.enabled && instance.elasticsearch_endpoint;
                        const s3Enabled = !!instance.s3_endpoint;
                        return `
                        <tr>
                            <td class="font-medium text-gray-900">${escapeHtml(instance.name)}</td>
                            <td class="text-gray-500 text-xs max-w-[200px] truncate">${escapeHtml(instance.base_url)}</td>
                            <td>
                                <div class="flex items-center gap-3">
                                    <span class="inline-flex items-center gap-1" title="Camunda">
                                        <span id="list-camunda-${escapeAttr(instance.id)}" class="w-2 h-2 rounded-full bg-gray-300"></span>
                                        <span class="text-xs text-gray-500">C8</span>
                                    </span>
                                    ${esEnabled ? `
                                    <span class="inline-flex items-center gap-1" title="Elasticsearch">
                                        <span id="list-es-${escapeAttr(instance.id)}" class="w-2 h-2 rounded-full bg-gray-300"></span>
                                        <span class="text-xs text-gray-500">ES</span>
                                    </span>` : ''}
                                    ${s3Enabled ? `
                                    <span class="inline-flex items-center gap-1" title="S3">
                                        <span id="list-s3-${escapeAttr(instance.id)}" class="w-2 h-2 rounded-full bg-gray-300"></span>
                                        <span class="text-xs text-gray-500">S3</span>
                                    </span>` : ''}
                                </div>
                            </td>
                            <td><span class="badge ${instance.enabled ? 'badge-scheduled' : 'badge-disabled'}">${instance.enabled ? 'Scheduled' : 'Unscheduled'}</span></td>
                            <td class="text-xs font-mono text-gray-500">${escapeHtml(instance.schedule || '—')}</td>
                            <td class="hidden lg:table-cell text-xs text-gray-500">${instance.last_backup_at ? formatTime(instance.last_backup_at) : '—'}</td>
                            <td class="hidden lg:table-cell">${instance.last_backup_status ? `<span class="badge badge-${instance.last_backup_status.toLowerCase()}">${instance.last_backup_status}</span>` : '—'}</td>
                            <td>
                                <div class="flex items-center gap-1">
                                    <button onclick="openInstanceForm(${escapeAttr(JSON.stringify(instance))})" title="Edit"
                                        class="p-1 text-gray-400 hover:text-blue-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/></svg>
                                    </button>
                                    <button onclick="toggleInstance('${escapeForInlineHandler(instance.id)}', ${!instance.enabled})" title="${instance.enabled ? 'Disable' : 'Enable'}"
                                        class="p-1 text-gray-400 hover:text-yellow-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="${instance.enabled ? 'M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636' : 'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z'}"/></svg>
                                    </button>
                                    <button onclick="confirmDeleteInstance('${escapeForInlineHandler(instance.id)}', '${escapeForInlineHandler(instance.name)}')" title="Delete"
                                        class="p-1 text-gray-400 hover:text-red-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                                    </button>
                                    <button onclick="triggerBackup('${escapeForInlineHandler(instance.id)}', '${escapeForInlineHandler(instance.name)}')" title="Trigger Backup"
                                        class="p-1 text-gray-400 hover:text-green-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"/></svg>
                                    </button>
                                </div>
                            </td>
                        </tr>`;
                    }).join('')}
                </tbody>
            </table>
        </div>
    `;

    // Fire connectivity checks for all listed instances
    checkInstanceListEndpoints(instances);
}

/**
 * Copies text to clipboard and shows a toast
 */
function copyToClipboard(text, label) {
    if (!text) return;
    navigator.clipboard.writeText(text).then(() => {
        showToast(`${label} copied to clipboard`, 'success');
    }).catch(err => {
        console.error('Failed to copy:', err);
        showToast('Failed to copy to clipboard', 'error');
    });
}

/**
 * Normalizes a Camunda instance ID for use in environment variable names.
 * Converts to uppercase and replaces hyphens with underscores.
 */
function normalizeForEnvVar(id) {
    return id.toUpperCase().replace(/-/g, '_');
}

/**
 * Updates the environment variable hints dynamically when creating a new instance
 */
function updateEnvVarHints(id) {
    if (!id) {
        document.querySelectorAll('.env-var-hint').forEach(el => el.classList.add('hidden'));
        return;
    }

    const normalized = normalizeForEnvVar(id);
    
    // Update ES hint
    const esHint = document.getElementById('es-env-hint');
    if (esHint) {
        const varName = `ELASTICSEARCH_PASSWORD_${normalized}`;
        esHint.querySelector('code').textContent = varName;
        esHint.querySelector('code').title = varName;
        esHint.querySelector('button').setAttribute('onclick', `copyToClipboard('${varName}', 'Variable name')`);
        esHint.classList.remove('hidden');
    }

    // Update ES snapshot repo hint
    const esRepoHint = document.getElementById('es-snapshot-repo-env-hint');
    if (esRepoHint) {
        esRepoHint.textContent = `ELASTICSEARCH_SNAPSHOT_REPOSITORY_${normalized}`;
    }

    // Update S3 hint
    const s3Hint = document.getElementById('s3-env-hint');
    if (s3Hint) {
        const varName = `S3_SECRETKEY_${normalized}`;
        s3Hint.querySelector('code').textContent = varName;
        s3Hint.querySelector('code').title = varName;
        s3Hint.querySelector('button').setAttribute('onclick', `copyToClipboard('${varName}', 'Variable name')`);
        s3Hint.classList.remove('hidden');
    }
}

// ============================================================
// Secret Fields (password inputs with show/hide)
// ============================================================

const EYE_ICON = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/></svg>';
const EYE_OFF_ICON = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l3.59 3.59m0 0A9.953 9.953 0 0112 5c4.478 0 8.268 2.943 9.543 7a10.025 10.025 0 01-4.132 5.411m0 0L21 21"/></svg>';

/**
 * Renders a password input with a show/hide toggle.
 * The saved value is never sent to the browser — an empty field means
 * "leave the stored credential unchanged".
 *
 * @param {Object} opts
 * @param {string} opts.name - Form field name
 * @param {string} opts.label - Visible label
 * @param {boolean} opts.isSet - Whether a credential is already stored server-side
 * @param {string} opts.checkType - Endpoint type to re-check on input ('elasticsearch' | 's3')
 * @param {string} opts.urlField - Name of the sibling endpoint URL field
 * @param {string} opts.statusId - Status dot element ID
 */
function secretFieldHtml({ name, label, isSet, checkType, urlField, statusId }) {
    const recheck = `onSecretInput(this, '${name}'); const urlInput = this.closest('form').querySelector('[name=${urlField}]'); if (urlInput && urlInput.value) checkEndpointStatus(urlInput, '${checkType}', '${statusId}')`;
    const note = isSet
        ? `Saved on the server. Leave blank to keep it, or <button type="button" class="text-red-600 hover:underline" onclick="clearSavedSecret(this, '${escapeAttr(name)}')">remove it</button>.`
        : 'Optional — leave blank to use the environment variable below.';
    return `
        <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">${escapeHtml(label)}</label>
            <div class="flex items-center gap-2">
                <input type="password" name="${escapeAttr(name)}" value="" autocomplete="new-password"
                    class="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="${isSet ? 'Saved — type to replace' : 'Enter value'}"
                    oninput="${escapeAttr(recheck)}">
                <button type="button" onclick="toggleSecretVisibility(this)" tabindex="-1"
                    class="p-1 text-gray-400 hover:text-gray-600 transition-colors"
                    title="Show value" aria-label="Show value">${EYE_ICON}</button>
            </div>
            <input type="hidden" name="${escapeAttr(name)}_cleared" value="">
            <p class="mt-1 text-xs text-gray-500" data-secret-note="${escapeAttr(name)}"
                data-secret-note-default="${escapeAttr(note)}">${note}</p>
        </div>`;
}

/**
 * Toggles a secret input between hidden and visible characters.
 */
function toggleSecretVisibility(button) {
    const input = button.parentElement.querySelector('input');
    if (!input) return;
    const reveal = input.type === 'password';
    input.type = reveal ? 'text' : 'password';
    button.innerHTML = reveal ? EYE_OFF_ICON : EYE_ICON;
    const title = reveal ? 'Hide value' : 'Show value';
    button.title = title;
    button.setAttribute('aria-label', title);
}

/**
 * Cancels a pending removal once the user types a replacement value.
 * A typed value always wins over the clear flag, so the note must not keep
 * claiming the secret will be removed.
 */
function onSecretInput(input, name) {
    if (!input.value) return;
    const form = input.closest('form');
    if (!form) return;
    const cleared = form.querySelector(`[name="${name}_cleared"]`);
    if (!cleared || cleared.value !== '1') return;
    cleared.value = '';
    const note = form.querySelector(`[data-secret-note="${name}"]`);
    if (note) {
        // Restore the original note so the "remove it" affordance stays available
        note.innerHTML = note.dataset.secretNoteDefault || '';
        note.className = 'mt-1 text-xs text-gray-500';
    }
}

/**
 * Marks a stored secret for removal on the next save.
 */
function clearSavedSecret(button, name) {
    const form = button.closest('form');
    if (!form) return;
    form.querySelector(`[name="${name}"]`).value = '';
    form.querySelector(`[name="${name}_cleared"]`).value = '1';
    const note = form.querySelector(`[data-secret-note="${name}"]`);
    if (note) {
        note.textContent = 'Saved value will be removed when you save.';
        note.className = 'mt-1 text-xs text-red-600';
    }
}

/**
 * Resolves what to send for a secret field:
 * a non-empty value sets it, an explicit clear sends "", otherwise undefined
 * (omitted from the payload) leaves the stored value untouched.
 */
function secretPayloadValue(fd, name) {
    const value = fd.get(name);
    if (value) return value;
    if (fd.get(`${name}_cleared`) === '1') return '';
    return undefined;
}

// ============================================================
// Endpoint Connectivity Check
// ============================================================
const _endpointCheckTimers = {};

/**
 * Status dot HTML component.
 * @param {string} id - Unique ID for the status indicator
 */
function statusDotHtml(id) {
    return `<span id="${id}" class="endpoint-status inline-flex items-center ml-2" title="Enter a URL to check connectivity">
        <span class="status-dot w-2.5 h-2.5 rounded-full bg-gray-300"></span>
        <span class="status-text text-xs text-gray-400 ml-1">Not checked</span>
    </span>`;
}

/**
 * Debounced endpoint connectivity check.
 * Called on input events from endpoint fields.
 */
function checkEndpointStatus(inputEl, type, statusId) {
    const url = inputEl.value.trim();
    const statusEl = document.getElementById(statusId);
    if (!statusEl) return;

    const dot = statusEl.querySelector('.status-dot');
    const text = statusEl.querySelector('.status-text');

    // Clear any pending timer for this field
    if (_endpointCheckTimers[statusId]) {
        clearTimeout(_endpointCheckTimers[statusId]);
    }

    if (!url) {
        dot.className = 'status-dot w-2.5 h-2.5 rounded-full bg-gray-300';
        text.textContent = 'Not checked';
        text.className = 'status-text text-xs text-gray-400 ml-1';
        statusEl.title = 'Enter a URL to check connectivity';
        return;
    }

    // Show checking state
    dot.className = 'status-dot w-2.5 h-2.5 rounded-full bg-gray-300 animate-pulse';
    text.textContent = 'Checking...';
    text.className = 'status-text text-xs text-gray-400 ml-1';

    // Debounce: wait 800ms after user stops typing
    _endpointCheckTimers[statusId] = setTimeout(async () => {
        try {
            // Gather credentials for ES checks
            const body = { url, type };

            // Include instance_id for env var lookup on backend
            const form = inputEl.closest('form');
            if (form) {
                const idField = form.querySelector('[name="id"]');
                if (idField && idField.value) {
                    body.instance_id = idField.value;
                }
                // For edit mode, instance_id may be in a hidden field or data attribute
                if (!body.instance_id) {
                    const hiddenId = form.querySelector('[data-instance-id]');
                    if (hiddenId) body.instance_id = hiddenId.dataset.instanceId;
                }
            }

            if (type === 'elasticsearch') {
                if (form) {
                    body.username = form.querySelector('[name="elasticsearch_username"]')?.value || '';
                    // Only sent when typed; otherwise the server resolves the stored credential
                    body.password = form.querySelector('[name="elasticsearch_password"]')?.value || '';
                }
            }
            if (type === 's3') {
                if (form) {
                    body.access_key = form.querySelector('[name="s3_accesskey"]')?.value || '';
                    body.secret_key = form.querySelector('[name="s3_secret_key"]')?.value || '';
                }
            }

            const result = await api.request('POST', 'api/check-endpoint', body);

            if (result.status === 'connected') {
                dot.className = 'status-dot w-2.5 h-2.5 rounded-full bg-green-500';
                text.textContent = result.message;
                text.className = 'status-text text-xs text-green-600 ml-1';
                statusEl.title = result.message;
            } else if (result.status === 'unauthenticated') {
                dot.className = 'status-dot w-2.5 h-2.5 rounded-full bg-yellow-400';
                text.textContent = result.message;
                text.className = 'status-text text-xs text-yellow-600 ml-1';
                statusEl.title = result.message;
            } else {
                dot.className = 'status-dot w-2.5 h-2.5 rounded-full bg-red-500';
                text.textContent = result.message;
                text.className = 'status-text text-xs text-red-600 ml-1';
                statusEl.title = result.message;
            }
        } catch (err) {
            dot.className = 'status-dot w-2.5 h-2.5 rounded-full bg-red-500';
            text.textContent = err.message || 'Check failed';
            text.className = 'status-text text-xs text-red-600 ml-1';
            statusEl.title = err.message || 'Failed to check endpoint';
        }
    }, 800);
}

/**
 * Updates a single status dot element in the instances list.
 */
function updateListDot(dotEl, status, message) {
    if (!dotEl) return;
    const colors = {
        connected: 'bg-green-500',
        unauthenticated: 'bg-yellow-400',
        unreachable: 'bg-red-500',
    };
    dotEl.className = `w-2 h-2 rounded-full ${colors[status] || 'bg-gray-300'}`;
    dotEl.title = message || status;
}

/**
 * Checks connectivity for all endpoints of instances shown in the list.
 * Fires requests in parallel for all instances.
 */
async function checkInstanceListEndpoints(instances) {
    if (!instances || instances.length === 0) return;

    for (const instance of instances) {
        // Always check Camunda base URL
        if (instance.base_url) {
            api.request('POST', 'api/check-endpoint', { url: instance.base_url, type: 'camunda', instance_id: instance.id })
                .then(r => updateListDot(document.getElementById(`list-camunda-${instance.id}`), r.status, r.message))
                .catch(e => updateListDot(document.getElementById(`list-camunda-${instance.id}`), 'unreachable', e.message || 'Check failed'));
        }

        // Check ES if enabled and endpoint configured
        const esComp = (instance.components || []).find(c => c.name === 'elasticsearch');
        if (esComp && esComp.enabled && instance.elasticsearch_endpoint) {
            api.request('POST', 'api/check-endpoint', {
                url: instance.elasticsearch_endpoint,
                type: 'elasticsearch',
                instance_id: instance.id,
                username: instance.elasticsearch_username || '',
            }).then(r => updateListDot(document.getElementById(`list-es-${instance.id}`), r.status, r.message))
              .catch(e => updateListDot(document.getElementById(`list-es-${instance.id}`), 'unreachable', e.message || 'Check failed'));
        }

        // Check S3 if endpoint configured
        if (instance.s3_endpoint) {
            api.request('POST', 'api/check-endpoint', {
                url: instance.s3_endpoint,
                type: 's3',
                instance_id: instance.id,
                access_key: instance.s3_accesskey || '',
            }).then(r => updateListDot(document.getElementById(`list-s3-${instance.id}`), r.status, r.message))
              .catch(e => updateListDot(document.getElementById(`list-s3-${instance.id}`), 'unreachable', e.message || 'Check failed'));
        }
    }
}

// ============================================================
// Instance Form (Modal)
// ============================================================
async function openInstanceForm(existingInstance) {
    const isEdit = !!existingInstance;
    let instance = existingInstance || {};

    // In create mode, fetch server-side defaults to pre-populate the form
    if (!isEdit) {
        try {
            const defaults = await api.get('api/defaults');
            instance = {
                schedule: defaults.schedule || '0 2 * * *',
                success_retention: defaults.success_retention || 7,
                failure_retention: defaults.failure_retention || 7,
                elasticsearch_endpoint: defaults.elasticsearch_endpoint || '',
                elasticsearch_username: defaults.elasticsearch_username || '',
                s3_endpoint: defaults.s3_endpoint || '',
                s3_accesskey: defaults.s3_accesskey || '',
            };
        } catch (e) {
            console.warn('Failed to fetch defaults, using hardcoded fallbacks:', e);
        }
    }

    const components = instance.components || [
        { name: 'zeebe', enabled: true },
        { name: 'operate', enabled: true },
        { name: 'tasklist', enabled: true },
        { name: 'optimize', enabled: false },
        { name: 'elasticsearch', enabled: true },
    ];

    const html = `
        <div class="bg-white rounded-xl shadow-xl w-full max-w-2xl">
            <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                <h2 class="text-lg font-semibold text-gray-900">${isEdit ? 'Edit' : 'Create'} Camunda Instance</h2>
                <button onclick="closeModal()" class="text-gray-400 hover:text-gray-600">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>
            <form id="instance-form" onsubmit="saveInstance(event, ${isEdit ? `'${escapeForInlineHandler(instance.id)}'` : 'null'})" class="px-6 py-4 space-y-4 max-h-[70vh] overflow-y-auto">
                
                <!-- Basic Info (expanded) -->
                <div class="border border-gray-200 rounded-lg">
                    <button type="button" class="accordion-toggle open w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-700" onclick="toggleAccordion(this)">
                        Basic Info
                        <svg class="accordion-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </button>
                    <div class="accordion-content open px-4 pb-4 space-y-3">
                        ${isEdit ? `<input type="hidden" name="id" value="${escapeAttr(instance.id || '')}">` : ''}
                        ${!isEdit ? `
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">ID <span class="text-red-500">*</span></label>
                            <input type="text" name="id" value="${escapeAttr(instance.id || '')}" required
                                pattern="^[a-z][a-z\\-]*[a-z]$|^[a-z]$"
                                title="Only lowercase letters and hyphens allowed. Must start and end with a letter."
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="my-camunda-instance"
                                oninput="this.value = this.value.toLowerCase().replace(/[^a-z-]/g, ''); updateEnvVarHints(this.value)">
                        </div>` : ''}
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Name <span class="text-red-500">*</span></label>
                            <input type="text" name="name" value="${escapeAttr(instance.name || '')}" required
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Production Camunda">
                        </div>
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Base URL <span class="text-red-500">*</span>${statusDotHtml('camunda-status')}</label>
                            <input type="url" name="base_url" value="${escapeAttr(instance.base_url || '')}" required
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="https://camunda.example.com"
                                oninput="checkEndpointStatus(this, 'camunda', 'camunda-status')">
                        </div>
                        <div class="grid grid-cols-2 gap-3">
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Schedule (cron)</label>
                                <input type="text" name="schedule" value="${escapeAttr(instance.schedule || '0 2 * * *')}"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    placeholder="0 2 * * *"
                                    pattern="^(\\*|[0-9]|[1-5][0-9])(\\/(\\d+))?\\s+(\\*|[0-9]|1[0-9]|2[0-3])(\\/(\\d+))?\\s+(\\*|[1-9]|[12][0-9]|3[01])(\\/(\\d+))?\\s+(\\*|[1-9]|1[0-2])(\\/(\\d+))?\\s+(\\*|[0-6])$"
                                    title="Enter a valid 5-field cron expression (min hour day month weekday)">
                                <p class="mt-1 text-xs text-gray-400">min hour day month weekday — e.g. 0 2 * * * (daily at 2 AM)</p>
                            </div>
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Success Retention</label>
                                <input type="number" name="success_retention" value="${instance.success_retention || 7}" min="1"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                            </div>
                        </div>
                        <div class="grid grid-cols-2 gap-3">
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Failure Retention</label>
                                <input type="number" name="failure_retention" value="${instance.failure_retention || 7}" min="1"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                            </div>
                        </div>
                        <div class="flex items-center gap-4">
                            <label class="flex items-center gap-2 text-sm">
                                <input type="checkbox" name="enabled" ${instance.enabled !== false ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
                                Schedule Enabled
                            </label>
                            <label class="flex items-center gap-2 text-sm">
                                <input type="checkbox" name="parallel_execution" ${instance.parallel_execution ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
                                Parallel Execution
                            </label>
                        </div>
                    </div>
                </div>

                <!-- Component Endpoints (collapsed) -->
                <div class="border border-gray-200 rounded-lg">
                    <button type="button" class="accordion-toggle w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-700" onclick="toggleAccordion(this)">
                        Component Endpoints
                        <svg class="accordion-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </button>
                    <div class="accordion-content px-4 pb-4 space-y-4">
                        ${CAMUNDA_COMPONENTS.map(comp => {
                            const compConf = components.find(c => c.name === comp) || { enabled: comp !== 'optimize' };
                            return `
                            <div class="border-t border-gray-100 pt-3">
                                <div class="flex items-center justify-between mb-2">
                                    <span class="text-sm font-medium text-gray-700 capitalize">${comp}</span>
                                    <label class="flex items-center gap-1 text-xs">
                                        <input type="checkbox" name="component_${comp}_enabled" ${compConf.enabled ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
                                        Enabled
                                    </label>
                                </div>
                                <div>
                                    <input type="text" name="${comp}_backup_endpoint" value="${escapeAttr(getInstanceEndpointValue(instance, comp, 'backup'))}"
                                        class="w-full px-2 py-1.5 border border-gray-300 rounded text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
                                        placeholder="Backup endpoint">
                                    <p class="mt-1 text-xs text-gray-400">Status endpoint is derived automatically (backup endpoint + /{backupId})</p>
                                </div>
                            </div>`;
                        }).join('')}
                    </div>
                </div>

                <!-- Elasticsearch (collapsed) -->
                <div class="border border-gray-200 rounded-lg">
                    <button type="button" class="accordion-toggle w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-700" onclick="toggleAccordion(this)">
                        Elasticsearch
                        <svg class="accordion-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </button>
                    <div class="accordion-content px-4 pb-4 space-y-3">
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Endpoint URL ${statusDotHtml('es-status')}</label>
                            <input type="text" name="elasticsearch_endpoint" value="${escapeAttr(instance.elasticsearch_endpoint || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="https://elasticsearch.example.com:9200"
                                oninput="checkEndpointStatus(this, 'elasticsearch', 'es-status')">
                        </div>
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Username</label>
                            <input type="text" name="elasticsearch_username" value="${escapeAttr(instance.elasticsearch_username || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="elastic"
                                oninput="const esInput = this.closest('form').querySelector('[name=elasticsearch_endpoint]'); if (esInput && esInput.value) checkEndpointStatus(esInput, 'elasticsearch', 'es-status')">
                        </div>
                        ${secretFieldHtml({
                            name: 'elasticsearch_password',
                            label: 'Password',
                            isSet: !!instance.elasticsearch_password_set,
                            checkType: 'elasticsearch',
                            urlField: 'elasticsearch_endpoint',
                            statusId: 'es-status',
                        })}
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Snapshot Repository</label>
                            <input type="text" name="elasticsearch_snapshot_repository" value="${escapeAttr(instance.elasticsearch_snapshot_repository || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="camunda-backup">
                            <p class="mt-1 text-xs text-gray-500">Leave blank to use the global default. Overridden by env var <code id="es-snapshot-repo-env-hint">ELASTICSEARCH_SNAPSHOT_REPOSITORY_${escapeHtml(normalizeForEnvVar(instance.id || '<ID>'))}</code>.</p>
                        </div>
                        <div id="es-env-hint" class="${instance.elasticsearch_password_env_var ? '' : 'hidden'} bg-blue-50 border border-blue-100 rounded-md p-3 env-var-hint">
                            <p class="text-xs text-blue-800 mb-1 font-medium">Environment Variable Alternative</p>
                            <p class="text-xs text-blue-600 mb-2">Instead of entering the password above, set this variable on the server. If set, it takes precedence:</p>
                            <div class="flex items-center gap-2">
                                <code class="flex-1 bg-white border border-blue-200 rounded px-2 py-1 text-xs font-mono text-blue-900 truncate" title="${escapeAttr(instance.elasticsearch_password_env_var || '')}">
                                    ${escapeHtml(instance.elasticsearch_password_env_var || '')}
                                </code>
                                <button type="button" onclick="copyToClipboard('${escapeForInlineHandler(instance.elasticsearch_password_env_var || '')}', 'Variable name')"
                                    class="p-1 text-blue-600 hover:text-blue-800 hover:bg-blue-100 rounded transition-colors" title="Copy variable name">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>
                                </button>
                            </div>
                        </div>
                        <div class="flex items-center gap-2">
                            ${(() => {
                                const esComp = components.find(c => c.name === 'elasticsearch') || { enabled: true };
                                return `<label class="flex items-center gap-1 text-xs">
                                    <input type="checkbox" name="component_elasticsearch_enabled" ${esComp.enabled ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
                                    Enable ES snapshots
                                </label>`;
                            })()}
                        </div>
                    </div>
                </div>

                <!-- S3 Configuration (collapsed) -->
                <div class="border border-gray-200 rounded-lg">
                    <button type="button" class="accordion-toggle w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-700" onclick="toggleAccordion(this)">
                        S3 Configuration
                        <svg class="accordion-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </button>
                    <div class="accordion-content px-4 pb-4 space-y-3">
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">S3 Endpoint <span class="text-red-500">*</span> ${statusDotHtml('s3-status')}</label>
                            <input type="text" name="s3_endpoint" value="${escapeAttr(instance.s3_endpoint || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="https://s3.amazonaws.com" required
                                oninput="checkEndpointStatus(this, 's3', 's3-status')">
                        </div>
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Access Key <span class="text-red-500">*</span></label>
                            <input type="text" name="s3_accesskey" value="${escapeAttr(instance.s3_accesskey || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="AKIAIOSFODNN7EXAMPLE" required
                                oninput="const s3Input = this.closest('form').querySelector('[name=s3_endpoint]'); if (s3Input && s3Input.value) checkEndpointStatus(s3Input, 's3', 's3-status')">
                        </div>
                        ${secretFieldHtml({
                            name: 's3_secret_key',
                            label: 'Secret Key',
                            isSet: !!instance.s3_secret_key_set,
                            checkType: 's3',
                            urlField: 's3_endpoint',
                            statusId: 's3-status',
                        })}
                        <div id="s3-env-hint" class="${instance.s3_secret_key_env_var ? '' : 'hidden'} bg-blue-50 border border-blue-100 rounded-md p-3 env-var-hint">
                            <p class="text-xs text-blue-800 mb-1 font-medium">Environment Variable Alternative</p>
                            <p class="text-xs text-blue-600 mb-2">Instead of entering the secret key above, set this variable on the server. If set, it takes precedence:</p>
                            <div class="flex items-center gap-2">
                                <code class="flex-1 bg-white border border-blue-200 rounded px-2 py-1 text-xs font-mono text-blue-900 truncate" title="${escapeAttr(instance.s3_secret_key_env_var || '')}">
                                    ${escapeHtml(instance.s3_secret_key_env_var || '')}
                                </code>
                                <button type="button" onclick="copyToClipboard('${escapeForInlineHandler(instance.s3_secret_key_env_var || '')}', 'Variable name')"
                                    class="p-1 text-blue-600 hover:text-blue-800 hover:bg-blue-100 rounded transition-colors" title="Copy variable name">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Exporter Configuration (collapsed) -->
                <div class="border border-gray-200 rounded-lg">
                    <button type="button" class="accordion-toggle w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-700" onclick="toggleAccordion(this)">
                        Exporter Configuration
                        <svg class="accordion-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </button>
                    <div class="accordion-content px-4 pb-4 space-y-3">
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Exporting Endpoint</label>
                            <input type="text" name="exporting_endpoint" value="${escapeAttr(instance.exporting_endpoint || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="http://zeebe:9600/actuator/exporting">
                            <p class="mt-1 text-xs text-gray-400">Base URL for the Zeebe exporting actuator. Leave empty to skip pause/resume during backups.</p>
                        </div>
                        <div>
                            <label class="flex items-center gap-2 text-sm">
                                <input type="checkbox" name="soft_export_pause" ${instance.soft_export_pause ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
                                Soft Pause
                            </label>
                            <p class="mt-1 text-xs text-gray-400">When enabled, appends ?soft=true to the pause request. A soft pause allows in-flight records to complete before pausing.</p>
                        </div>
                    </div>
                </div>
            </form>
            <div class="px-6 py-4 border-t border-gray-200 flex justify-end gap-2">
                <button onclick="closeModal()" class="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50">
                    Cancel
                </button>
                <button onclick="document.getElementById('instance-form').requestSubmit()" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700">
                    ${isEdit ? 'Update' : 'Create'}
                </button>
            </div>
        </div>
    `;

    showModal(html);
}

async function saveInstance(event, editId) {
    event.preventDefault();
    const form = event.target;
    const fd = new FormData(form);

    const components = [...CAMUNDA_COMPONENTS, 'elasticsearch'].map(name => ({
        name,
        enabled: fd.get(`component_${name}_enabled`) === 'on',
    }));

    const schedule = fd.get('schedule');
    if (schedule && !isValidCron(schedule)) {
        showToast('Invalid cron expression. Expected format: min hour day month weekday', 'error');
        return;
    }

    const payload = {
        id: editId || fd.get('id'),
        name: fd.get('name'),
        base_url: fd.get('base_url'),
        enabled: fd.get('enabled') === 'on',
        schedule,
        success_retention: parseInt(fd.get('success_retention')) || 7,
        failure_retention: parseInt(fd.get('failure_retention')) || 7,
        parallel_execution: fd.get('parallel_execution') === 'on',
        components,
        // Dynamically populate all component endpoint fields from the constant
        ...Object.fromEntries(
            COMPONENT_ENDPOINT_FIELDS.map(field => [field, fd.get(field) || ''])
        ),
        elasticsearch_endpoint: fd.get('elasticsearch_endpoint') || '',
        elasticsearch_username: fd.get('elasticsearch_username') || '',
        elasticsearch_snapshot_repository: fd.get('elasticsearch_snapshot_repository') || '',
        s3_endpoint: fd.get('s3_endpoint') || '',
        s3_accesskey: fd.get('s3_accesskey') || '',
        exporting_endpoint: fd.get('exporting_endpoint') || '',
        soft_export_pause: fd.get('soft_export_pause') === 'on',
    };

    // Secrets are omitted unless the user typed a new value or cleared the saved one
    for (const field of ['elasticsearch_password', 's3_secret_key']) {
        const value = secretPayloadValue(fd, field);
        if (value !== undefined) payload[field] = value;
    }

    try {
        if (editId) {
            await api.put(`api/camundas/${editId}`, payload);
            showToast('Instance updated successfully', 'success');
        } else {
            await api.post('api/camundas', payload);
            showToast('Instance created successfully', 'success');
        }
        closeModal();
        invalidateInstancesCache();
        loadInstances();
    } catch (err) {
        showToast(err.message || 'Failed to save instance', 'error');
    }
}

async function toggleInstance(id, enable) {
    try {
        await api.post(`api/camundas/${id}/${enable ? 'enable' : 'disable'}`);
        showToast(`Instance ${enable ? 'enabled' : 'disabled'}`, 'success');
        invalidateInstancesCache();
        loadInstances();
    } catch (err) {
        showToast(err.message || 'Failed to update instance', 'error');
    }
}

async function confirmDeleteInstance(id, name) {
    const confirmed = await showConfirm(`Are you sure you want to delete "${name}"? This action cannot be undone.`);
    if (!confirmed) return;

    try {
        await api.del(`api/camundas/${id}`);
        showToast('Instance deleted', 'success');
        invalidateInstancesCache();
        loadInstances();
    } catch (err) {
        showToast(err.message || 'Failed to delete instance', 'error');
    }
}

// ============================================================
// Backups Tab
// ============================================================
async function loadBackupsTab() {
    // Use cached instances when available to avoid redundant API calls
    try {
        const instances = await getInstances();
        renderBackupsTabControls(instances);

        if (instances.length === 0) {
            renderBackupsEmptyState();
            return;
        }

        // Reset selection if it points to an instance that no longer exists
        let sel = state.selectedInstanceId;
        if (!instances.some(i => i.id === sel)) sel = instances[0].id;
        state.selectedInstanceId = sel;
        const select = document.getElementById('backup-instance-select');
        if (select) select.value = sel;
        loadBackups(sel, state.backupFilter);
    } catch (err) {
        const el = document.getElementById('backups-table-container');
        if (el) el.innerHTML = `<div class="empty-state"><p>Failed to load instances</p></div>`;
    }
}

function renderBackupsTabControls(instances) {
    const controls = document.getElementById('backups-controls');
    if (!controls) return;

    // With no instances there is nothing to select or filter — hide the controls
    // entirely so we don't show an empty, unusable dropdown.
    if (!instances || instances.length === 0) {
        controls.innerHTML = '';
        return;
    }

    controls.innerHTML = `
        <div class="flex flex-col sm:flex-row items-start sm:items-center gap-3">
            <select id="backup-instance-select" onchange="onBackupInstanceChange(this.value)"
                class="px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
                ${instances.map(i => `<option value="${i.id}" ${i.id === state.selectedInstanceId ? 'selected' : ''}>${escapeHtml(i.name)}</option>`).join('')}
            </select>
            <div class="flex gap-1 flex-wrap">
                ${['all', 'completed', 'failed', 'incomplete', 'orphaned'].map(f => `
                    <button onclick="filterBackups('${f}')"
                        class="px-3 py-1 text-xs font-medium rounded-full transition-colors ${state.backupFilter === f ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}"
                        data-filter="${f}">
                        ${f.charAt(0).toUpperCase() + f.slice(1)}
                    </button>
                `).join('')}
            </div>
            <button id="backups-refresh" onclick="refreshBackups()" type="button"
                title="Refresh: reload backups and rescan for orphaned ones"
                aria-label="Refresh backups"
                class="backups-refresh-btn p-2 text-gray-400 hover:text-blue-600 rounded-md hover:bg-gray-100 transition-colors">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                        d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                </svg>
            </button>
        </div>
    `;
}

// The refresh control on the Backups tab. It rescans as well as reloading,
// because orphaned rows come from a scan rather than from backup history, so
// re-fetching the history alone would leave them stale on every tab.
async function refreshBackups() {
    const instanceId = state.selectedInstanceId;
    if (!instanceId) return;

    const btn = document.getElementById('backups-refresh');
    btn?.classList.add('is-spinning');
    if (btn) btn.disabled = true;

    try {
        const report = await api.post(`api/camundas/${instanceId}/backups/reconcile`);
        report._instanceId = instanceId;
        reconcileReport = report;
        await getReasonCatalog();
    } catch (err) {
        // A failed scan must not block the reload: the backup history is still
        // worth showing, just without fresh orphan data.
        showToast(err.message || 'Scan failed; showing backups without a fresh scan', 'error');
    } finally {
        btn?.classList.remove('is-spinning');
        if (btn) btn.disabled = false;
    }

    await loadBackups(instanceId, state.backupFilter);
}

function renderBackupsEmptyState() {
    const el = document.getElementById('backups-table-container');
    if (!el) return;
    el.innerHTML = `
        <div class="empty-state">
            <svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"/></svg>
            <p class="text-lg font-medium text-gray-600">No Instances Configured</p>
            <p class="text-sm mt-1">Add a Camunda instance to start viewing backups. <a href="#" onclick="showTab('instances'); return false;" class="text-blue-600 hover:underline">Add one</a>.</p>
        </div>
    `;
}

function onBackupInstanceChange(instanceId) {
    state.selectedInstanceId = instanceId;
    loadBackups(instanceId, state.backupFilter);
}

function filterBackups(filter) {
    state.backupFilter = filter;

    // Update filter button styles
    document.querySelectorAll('[data-filter]').forEach(btn => {
        const isActive = btn.dataset.filter === filter;
        btn.className = `px-3 py-1 text-xs font-medium rounded-full transition-colors ${isActive ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`;
    });

    if (state.selectedInstanceId) {
        loadBackups(state.selectedInstanceId, filter);
    }
}

async function loadBackups(instanceId, filter) {
    const el = document.getElementById('backups-table-container');
    if (!el) return;

    el.innerHTML = '<div class="flex justify-center py-8"><div class="spinner"></div></div>';

    let path;
    switch (filter) {
        case 'completed': path = `api/camundas/${instanceId}/backups?status=COMPLETED`; break;
        case 'failed': path = `api/camundas/${instanceId}/backups/failed`; break;
        case 'incomplete': path = `api/camundas/${instanceId}/backups/incomplete`; break;
        default: path = `api/camundas/${instanceId}/backups`;
    }

    // Orphans have no history record, so they never come back from the backup
    // endpoints. They are folded in from the reconciliation report instead.
    if (filter === 'orphaned') {
        await loadOrphanedBackups(instanceId);
        return;
    }

    try {
        const backups = await api.get(path);
        let rows = backups || [];

        // "All" means all backups, orphans included.
        if (filter === 'all' || !filter) {
            const known = new Set(rows.map(b => b.backup_id));
            // A report can be older than the backup list it is merged into, so a
            // backup recorded since the last scan would otherwise appear twice -
            // once real, once as a stale orphan. The live list always wins.
            const orphans = (await loadOrphanRows(instanceId)).filter(o => !known.has(o.backup_id));
            rows = rows.concat(orphans);
            rows.sort((a, b) => new Date(b.start_time || 0) - new Date(a.start_time || 0));
        }

        // A tracked backup with findings keeps its real status; the marker is how
        // the user learns it has a problem worth opening.
        annotateBackupIssues(rows);
        renderBackupsTable(instanceId, rows, { scanNote: (filter === 'all' || !filter) });
    } catch (err) {
        el.innerHTML = `<div class="empty-state"><p>Failed to load backups</p><p class="text-sm mt-1">${err.message}</p></div>`;
    }
}

// One line on the All tab explaining where the ORPHANED rows came from.
function renderScanNote(instanceId) {
    if (!reconcileReport) {
        return `
            <div class="text-xs text-gray-500 mb-3">
                Orphaned backups are not shown: no scan has run yet. Use the refresh button to scan.
            </div>
        `;
    }
    const unreachable = Object.values(reconcileReport.sources_checked || {})
        .filter(s => !s.reachable && !s.skipped).map(s => s.name);
    const warn = unreachable.length
        ? ` Could not check ${escapeHtml(unreachable.join(', '))}, so some orphans may be missing.`
        : '';
    return `
        <div class="text-xs text-gray-500 mb-3">
            Orphaned backups from the scan of ${escapeHtml(formatTime(reconcileReport.finished_at))}.${warn}
        </div>
    `;
}

function renderBackupsTable(instanceId, backups, opts = {}) {
    const el = document.getElementById('backups-table-container');
    if (!el) return;

    // Present on the Orphaned tab: scan time, per-source health and any
    // instance-wide findings.
    let header = opts.report ? renderReconcileHeader(instanceId, opts.report) : '';

    // On the All tab the orphan rows come from a scan rather than from backup
    // history, so their freshness and completeness need saying somewhere.
    if (opts.scanNote) header = renderScanNote(instanceId) + header;

    if (backups.length === 0) {
        const report = opts.report;
        const partial = report && !Object.values(report.sources_checked || {})
            .every(s => s.reachable || s.skipped);
        const title = report
            ? (partial ? 'No issues found in the sources that could be checked' : 'No orphaned backups found')
            : 'No Backups Found';
        const sub = report
            ? (partial
                ? 'Re-run the scan once every source is reachable to confirm.'
                : 'Every backup in Zeebe, the Camunda components and Elasticsearch has a matching record.')
            : 'No backups match the current filter.';
        el.innerHTML = header + `
            <div class="empty-state">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"/></svg>
                <p class="text-lg font-medium text-gray-600">${escapeHtml(title)}</p>
                <p class="text-sm mt-1">${escapeHtml(sub)}</p>
            </div>
        `;
        return;
    }

    el.innerHTML = header + `
        <div class="overflow-x-auto">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Backup ID</th>
                        <th>Start Time</th>
                        <th class="hidden sm:table-cell">End Time</th>
                        <th class="hidden md:table-cell">Duration</th>
                        <th>Status</th>
                        <th class="hidden lg:table-cell">Trigger</th>
                        <th class="hidden md:table-cell">Components</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${backups.map(b => `
                        <tr>
                            <td class="font-mono text-xs font-medium text-gray-900">${escapeHtml(b.backup_id)}</td>
                            <td class="text-xs text-gray-500">${b.start_time ? formatTime(b.start_time) : '—'}</td>
                            <td class="hidden sm:table-cell text-xs text-gray-500">${b.end_time ? formatTime(b.end_time) : '—'}</td>
                            <td class="hidden md:table-cell text-xs text-gray-500">${b.duration_seconds != null ? formatDuration(b.duration_seconds) : '—'}</td>
                            <td>
                                <span class="badge badge-${b.status.toLowerCase()}">${b.status}</span>
                                ${b._issue ? `<span class="issue-marker severity-${escapeHtml(b._issue.severity)}" title="${escapeHtml(reasonInfo(b._issue.primary_reason).label)}">!</span>` : ''}
                            </td>
                            <td class="hidden lg:table-cell">${b.trigger_type ? `<span class="badge badge-${b.trigger_type.toLowerCase()}">${b.trigger_type}</span>` : '<span class="text-xs text-gray-400">—</span>'}</td>
                            <td class="hidden md:table-cell text-xs text-gray-500">
                                ${b.backup_stats ? `<span class="text-green-600">${b.backup_stats.successful_components}</span>/<span class="text-red-600">${b.backup_stats.failed_components}</span>/<span>${b.backup_stats.total_components}</span>` : '—'}
                            </td>
                            <td>
                                <div class="flex items-center gap-1">
                                    <button onclick="${b.status === 'ORPHANED'
                                            ? `showOrphanedBackupDetail('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(b.backup_id)}')`
                                            : `showBackupDetail('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(b.backup_id)}')`}" title="View Details"
                                        class="p-1 text-gray-400 hover:text-blue-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/></svg>
                                    </button>
                                    ${b.status === 'ORPHANED' ? '' : `
                                    <button onclick="viewBackupLogs('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(b.backup_id)}')" title="View Logs"
                                        class="p-1 text-gray-400 hover:text-green-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
                                    </button>`}
                                    ${['FAILED', 'INCOMPLETE', 'COMPLETED'].includes(b.status) ? `
                                    <button onclick="confirmDeleteBackup('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(b.backup_id)}')" title="Delete"
                                        class="p-1 text-gray-400 hover:text-red-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                                    </button>` : ''}
                                </div>
                            </td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

// ============================================================
// Backup Detail & Logs
// ============================================================
async function showBackupDetail(instanceId, backupId) {
    try {
        const backup = await api.get(`api/camundas/${instanceId}/backups/${backupId}`);
        const components = backup.components || {};

        const html = `
            <div class="bg-white rounded-xl shadow-xl w-full max-w-3xl">
                <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                    <div>
                        <h2 class="text-lg font-semibold text-gray-900">Backup Details</h2>
                        <p class="text-sm text-gray-500 font-mono">${escapeHtml(backupId)}</p>
                    </div>
                    <button onclick="closeModal()" class="text-gray-400 hover:text-gray-600">
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                    </button>
                </div>
                <div class="px-6 py-4 space-y-4 max-h-[70vh] overflow-y-auto">
                    <!-- Summary -->
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div><span class="text-xs text-gray-500 block">Status</span><span class="badge badge-${backup.status.toLowerCase()}">${backup.status}</span></div>
                        <div><span class="text-xs text-gray-500 block">Trigger</span><span class="badge badge-${backup.trigger_type.toLowerCase()}">${backup.trigger_type}</span></div>
                        <div><span class="text-xs text-gray-500 block">Start</span><span class="text-sm">${formatTime(backup.start_time)}</span></div>
                        <div><span class="text-xs text-gray-500 block">Duration</span><span class="text-sm">${backup.duration_seconds != null ? formatDuration(backup.duration_seconds) : '—'}</span></div>
                    </div>

                    ${backup.error_message ? `<div class="bg-red-50 border border-red-200 rounded-md p-3 text-sm text-red-700">${escapeHtml(backup.error_message)}</div>` : ''}

                    ${renderTrackedBackupFindings(backupId)}

                    <!-- Components -->
                    <div>
                        <h3 class="text-sm font-semibold text-gray-700 mb-2">Components</h3>
                        <div class="space-y-2">
                            ${Object.entries(components).map(([name, comp]) => `
                                <div class="border border-gray-200 rounded-md p-3">
                                    <div class="flex items-center justify-between mb-1">
                                        <span class="text-sm font-medium capitalize">${escapeHtml(name)}</span>
                                        <span class="badge badge-${comp.status.toLowerCase()}">${comp.status}</span>
                                    </div>
                                    <div class="grid grid-cols-2 gap-2 text-xs text-gray-500">
                                        <div>Enabled: ${comp.enabled ? 'Yes' : 'No'}</div>
                                        <div>Duration: ${(comp.end_time && comp.status !== 'SKIPPED') ? formatDuration(comp.duration_seconds || 0) : '—'}</div>
                                        ${comp.snapshot_name ? `<div>Snapshot: ${escapeHtml(comp.snapshot_name)}</div>` : ''}
                                        ${comp.snapshot_repository ? `<div>Repository: ${escapeHtml(comp.snapshot_repository)}</div>` : ''}
                                    </div>
                                    ${comp.error_message ? `<p class="mt-1 text-xs text-red-600">${escapeHtml(comp.error_message)}</p>` : ''}
                                </div>
                            `).join('')}
                        </div>
                    </div>

                    <!-- Metadata -->
                    ${backup.metadata ? `
                    <div>
                        <h3 class="text-sm font-semibold text-gray-700 mb-2">Metadata</h3>
                        <div class="text-xs text-gray-500 grid grid-cols-2 gap-1">
                            <div>Controller: ${escapeHtml(backup.metadata.controller_version || '—')}</div>
                            <div>Config: ${escapeHtml(backup.metadata.config_version || '—')}</div>
                            <div>Mode: ${escapeHtml(backup.metadata.execution_mode || '—')}</div>
                            <div>Reason: ${escapeHtml(backup.metadata.backup_reason || '—')}</div>
                        </div>
                    </div>` : ''}
                </div>
                <div class="px-6 py-4 border-t border-gray-200 flex justify-between">
                    <button onclick="viewBackupLogs('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(backupId)}')" class="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50">
                        View Logs
                    </button>
                    <button onclick="closeModal()" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700">
                        Close
                    </button>
                </div>
            </div>
        `;
        showModal(html);
    } catch (err) {
        showToast(err.message || 'Failed to load backup details', 'error');
    }
}

async function viewBackupLogs(instanceId, backupId) {
    try {
        const logs = await api.get(`api/camundas/${instanceId}/backups/${backupId}/logs`);
        const html = `
            <div class="bg-white rounded-xl shadow-xl w-full max-w-4xl">
                <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                    <div>
                        <h2 class="text-lg font-semibold text-gray-900">Backup Logs</h2>
                        <p class="text-sm text-gray-500 font-mono">${escapeHtml(backupId)}</p>
                    </div>
                    <button onclick="closeModal()" class="text-gray-400 hover:text-gray-600">
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                    </button>
                </div>
                <div class="p-4">
                    <div class="log-viewer">${escapeHtml(logs || 'No log content available.')}</div>
                </div>
                <div class="px-6 py-4 border-t border-gray-200 flex justify-end">
                    <button onclick="closeModal()" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700">
                        Close
                    </button>
                </div>
            </div>
        `;
        showModal(html);
    } catch (err) {
        showToast(err.message || 'Failed to load backup logs', 'error');
    }
}

async function confirmDeleteBackup(instanceId, backupId) {
    const confirmed = await showConfirm(`Delete backup ${backupId}? This action cannot be undone.`);
    if (!confirmed) return;

    try {
        await api.del(`api/camundas/${instanceId}/backups/${backupId}`);
        showToast('Backup deleted', 'success');
        loadBackups(instanceId, state.backupFilter);
    } catch (err) {
        showToast(err.message || 'Failed to delete backup', 'error');
    }
}

// ============================================================
// Modal
// ============================================================
/** Element that held focus before the modal opened, restored on close. */
let _priorFocusEl = null;

function initModalListeners() {
    // Backdrop click — use delegation on the backdrop element itself
    const backdrop = document.getElementById('modal-backdrop');
    if (backdrop) {
        backdrop.addEventListener('click', (e) => {
            if (e.target === backdrop) closeModal();
        });
    }

    // Escape key
    document.addEventListener('keydown', _handleModalKeydown);
}

function _handleModalKeydown(e) {
    const panel = document.getElementById('modal-panel');
    if (!panel || !panel.classList.contains('active')) return;

    if (e.key === 'Escape') {
        closeModal();
        return;
    }

    // Focus trap: keep Tab / Shift+Tab inside the modal
    if (e.key === 'Tab') {
        const focusable = panel.querySelectorAll(
            'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
        );
        if (focusable.length === 0) return;

        const first = focusable[0];
        const last = focusable[focusable.length - 1];

        if (e.shiftKey) {
            if (document.activeElement === first) {
                e.preventDefault();
                last.focus();
            }
        } else {
            if (document.activeElement === last) {
                e.preventDefault();
                first.focus();
            }
        }
    }
}

function _announceModal(message) {
    let region = document.getElementById('modal-live-region');
    if (!region) {
        region = document.createElement('div');
        region.id = 'modal-live-region';
        region.setAttribute('aria-live', 'polite');
        region.setAttribute('role', 'status');
        region.className = 'sr-only';
        region.style.cssText = 'position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;';
        document.body.appendChild(region);
    }
    region.textContent = message;
}

function showModal(contentHtml) {
    const backdrop = document.getElementById('modal-backdrop');
    const panel = document.getElementById('modal-panel');
    if (!backdrop || !panel) return;

    // Remember the element that had focus so we can restore it later
    _priorFocusEl = document.activeElement;

    panel.innerHTML = contentHtml;

    // Ensure the panel is marked as a dialog for assistive tech
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-modal', 'true');

    // Trigger reflow then add active class
    requestAnimationFrame(() => {
        backdrop.classList.add('active');
        panel.classList.add('active');

        // Move focus to the first focusable element inside the modal
        const firstFocusable = panel.querySelector(
            'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
        );
        if (firstFocusable) firstFocusable.focus();
    });

    _announceModal('Dialog opened');
}

function closeModal() {
    const backdrop = document.getElementById('modal-backdrop');
    const panel = document.getElementById('modal-panel');
    if (!backdrop || !panel) return;

    backdrop.classList.remove('active');
    panel.classList.remove('active');

    panel.removeAttribute('role');
    panel.removeAttribute('aria-modal');

    setTimeout(() => { panel.innerHTML = ''; }, MODAL_TRANSITION_MS);

    // Restore focus to the element that was focused before the modal opened
    if (_priorFocusEl && typeof _priorFocusEl.focus === 'function') {
        _priorFocusEl.focus();
        _priorFocusEl = null;
    }

    _announceModal('Dialog closed');
}

// ============================================================
// Confirm Dialog
// ============================================================
let _pendingConfirm = null;

function showConfirm(message) {
    // If a confirm is already pending, reject it before opening a new one
    if (_pendingConfirm) {
        _pendingConfirm.resolve(false);
        _pendingConfirm = null;
    }

    return new Promise((resolve) => {
        const confirmId = Date.now();

        _pendingConfirm = { id: confirmId, resolve };

        const html = `
            <div class="bg-white rounded-xl shadow-xl w-full max-w-sm">
                <div class="p-6">
                    <div class="flex items-center gap-3 mb-4">
                        <div class="flex-shrink-0 w-10 h-10 bg-red-100 rounded-full flex items-center justify-center">
                            <svg class="w-5 h-5 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"/></svg>
                        </div>
                        <p class="text-sm text-gray-700">${escapeHtml(message)}</p>
                    </div>
                    <div class="flex justify-end gap-2">
                        <button onclick="resolveConfirm(false)" class="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
                        <button onclick="resolveConfirm(true)" class="px-3 py-1.5 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700">Delete</button>
                    </div>
                </div>
            </div>
        `;

        showModal(html);
    });
}

function resolveConfirm(result) {
    if (!_pendingConfirm) return;
    const { resolve } = _pendingConfirm;
    _pendingConfirm = null;
    closeModal();
    resolve(result);
}

// ============================================================
// Toast Notifications
// ============================================================
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const icons = {
        success: '<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>',
        error: '<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>',
        info: '<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>',
        warning: '<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"/></svg>',
    };

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `${icons[type] || icons.info}<span class="text-sm">${escapeHtml(message)}</span>`;
    container.appendChild(toast);

    // Auto-remove after timeout
    setTimeout(() => {
        toast.classList.add('removing');
        setTimeout(() => toast.remove(), TOAST_REMOVAL_ANIMATION_MS);
    }, TOAST_DURATION_MS);
}

// ============================================================
// Accordion Toggle
// ============================================================
function toggleAccordion(btn) {
    btn.classList.toggle('open');
    const content = btn.nextElementSibling;
    if (content) content.classList.toggle('open');
}

// ============================================================
// Utility Functions
// ============================================================

/**
 * Escape functions — use the right one for each context:
 *
 *   escapeHtml(str)              — text content between HTML tags
 *   escapeAttr(str)              — HTML attribute values (e.g. value="...")
 *   escapeForInlineHandler(str)  — values inside inline event handlers
 *                                  (e.g. onclick="fn('...')")
 */

/**
 * Safely retrieves a component endpoint value from an instance object.
 * Returns empty string if the field doesn't exist or the component name is invalid.
 *
 * @param {Object} instance - The instance data object
 * @param {string} componentName - Component name (e.g. 'zeebe', 'operate')
 * @param {string} endpointType - Must be 'backup' (status is derived from the backup endpoint)
 * @returns {string}
 */
function getInstanceEndpointValue(instance, componentName, endpointType) {
    const fieldName = `${componentName}_${endpointType}_endpoint`;
    if (!COMPONENT_ENDPOINT_FIELDS.includes(fieldName)) {
        console.warn(`Unknown endpoint field: ${fieldName}`);
        return '';
    }
    return instance[fieldName] || '';
}

function escapeHtml(str) {
    if (str == null) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function escapeAttr(str) {
    if (str == null) return '';
    return String(str).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/**
 * Escapes a string for safe interpolation inside a single-quoted JavaScript
 * string literal within an HTML attribute (e.g. onclick="fn('VALUE')").
 *
 * Two-layer escaping:
 *  1. JS string escape: \ ' newlines
 *  2. HTML attribute escape: & " < >
 *
 * The browser first decodes HTML entities in the attribute, then evaluates
 * the result as JavaScript — so both layers are required.
 */
function escapeForInlineHandler(str) {
    if (str == null) return '';
    // Layer 1: escape for JavaScript single-quoted string literal
    let s = String(str)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'") 
        .replace(/\n/g, '\\n')
        .replace(/\r/g, '\\r');
    // Layer 2: escape for HTML attribute context
    return s
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

/**
 * Validates a 5-field cron expression (min hour day month weekday).
 * Supports numbers, asterisk, ranges (1-5), lists (1,3,5), and steps.
 */
function isValidCron(expr) {
    if (!expr || typeof expr !== 'string') return false;
    const fields = expr.trim().split(/\s+/);
    if (fields.length !== 5) return false;

    const ranges = [
        [0, 59],  // minute
        [0, 23],  // hour
        [1, 31],  // day of month
        [1, 12],  // month
        [0, 6],   // day of week
    ];

    return fields.every((field, i) => {
        const [min, max] = ranges[i];
        // Each field can be a comma-separated list of atoms
        return field.split(',').every(atom => {
            // step: */2, 1-5/2, or plain number/range
            const [value, step] = atom.split('/');
            if (step !== undefined && (!/^\d+$/.test(step) || Number(step) === 0)) return false;
            if (value === '*') return true;
            // range: 1-5
            if (value.includes('-')) {
                const [lo, hi] = value.split('-');
                if (!/^\d+$/.test(lo) || !/^\d+$/.test(hi)) return false;
                return Number(lo) >= min && Number(hi) <= max && Number(lo) <= Number(hi);
            }
            // single number
            if (!/^\d+$/.test(value)) return false;
            const n = Number(value);
            return n >= min && n <= max;
        });
    });
}

function formatTime(isoString) {
    if (!isoString) return '—';
    try {
        const d = new Date(isoString);
        return d.toLocaleString(undefined, {
            month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
        });
    } catch (err) {
        console.warn('Failed to parse date:', isoString, err);
        return isoString;
    }
}

function formatDuration(seconds) {
    if (seconds == null) return '—';
    if (seconds < 60) return `${seconds}s`;
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    if (m < 60) return `${m}m ${s}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
}

// ============================================================
// Reconciliation (orphaned backup detection)
// ============================================================

// Cached once. The catalogue is static, so reports carry only reason codes and
// the human text is looked up here.
let reasonCatalog = null;

async function getReasonCatalog() {
    if (reasonCatalog) return reasonCatalog;
    try {
        const entries = await api.get('api/reconcile/reasons');
        reasonCatalog = {};
        (entries || []).forEach(e => { reasonCatalog[e.code] = e; });
    } catch (err) {
        console.warn('Failed to load reason catalog:', err);
        reasonCatalog = {};
    }
    return reasonCatalog;
}

// Turns a catalogue remediation into something runnable by substituting the
// values this scan actually observed. A "What to do" box full of {placeholders}
// is not advice, it is homework.
function concreteRemediation(text, issue) {
    if (!text) return text;
    const report = reconcileReport || {};

    let out = text;
    if (report.snapshot_repository) {
        out = out.replaceAll('{repository}', report.snapshot_repository);
    }
    if (issue?.backup_id) {
        out = out.replaceAll('{backup_id}', issue.backup_id);
        // The controller names its own snapshots after the backup ID; component
        // snapshots are multi-part, so only substitute when the detail text
        // pinned an exact name.
        const named = (issue.reasons || [])
            .map(f => (f.detail || '').match(/snapshot "([^"]+)"/))
            .find(Boolean);
        if (named) out = out.replaceAll('{snapshot_name}', named[1]);
    }
    const endpoints = report.component_endpoints || {};
    const component = (issue?.present_in || []).find(c => endpoints[c]);
    if (component) {
        out = out.replaceAll('{component_backup_endpoint}', endpoints[component]);
    }
    return out;
}

// The exact commands that would remove this backup's surviving artifacts, one
// per source it was actually found in. Derived from the scan rather than from
// catalogue prose, so it stays correct for findings that span several sources.
function remediationCommands(issue) {
    const report = reconcileReport || {};
    const endpoints = report.component_endpoints || {};
    const cmds = [];

    (issue?.present_in || []).forEach(source => {
        if (source === 'elasticsearch') {
            const named = (issue.reasons || [])
                .map(f => (f.detail || '').match(/snapshot "([^"]+)"/))
                .find(Boolean);
            const name = named ? named[1] : issue.backup_id;
            const repo = report.snapshot_repository || '{repository}';
            cmds.push(`DELETE /_snapshot/${repo}/${name}`);
        } else if (endpoints[source]) {
            cmds.push(`DELETE ${endpoints[source]}/${issue.backup_id}`);
        }
    });
    return cmds;
}

// Several findings can share a reason code, differing only in which source they
// were observed on. Rendering them as separate cards repeats the same title and
// advice, so they are grouped into one card with the details listed under it.
function groupFindingsByReason(reasons) {
    const groups = new Map();
    (reasons || []).forEach(f => {
        if (!groups.has(f.reason)) groups.set(f.reason, { reason: f.reason, severity: f.severity, details: [] });
        if (f.detail) groups.get(f.reason).details.push(f.detail);
    });
    return [...groups.values()];
}

// showCommands is only ever true for orphans. A tracked backup has a Delete
// action that goes through the retention manager and its never-delete-the-most-
// recent-backup guard; offering raw DELETE calls alongside it would invite the
// user to route around that check.
function renderFindingCards(issue, { showCommands = false } = {}) {
    const groups = groupFindingsByReason(issue.reasons);
    const cmds = showCommands ? remediationCommands(issue) : [];

    return groups.map(g => {
        const info = reasonInfo(g.reason);
        const details = g.details.length === 1
            ? `<p class="text-xs text-gray-600">${escapeHtml(g.details[0])}</p>`
            : `<ul class="finding-details">${g.details.map(d => `<li>${escapeHtml(d)}</li>`).join('')}</ul>`;

        return `
            <div class="border border-gray-200 rounded-md p-3 bg-white">
                <div class="flex items-center justify-between mb-1 gap-2 flex-wrap">
                    <span class="text-sm font-medium">${escapeHtml(info.label)}</span>
                    <span class="severity-badge severity-${escapeHtml(g.severity)}">${escapeHtml(g.severity.replace('_', ' '))}</span>
                </div>
                <div class="reason-code mb-1">${escapeHtml(g.reason)}</div>
                ${details}
                ${info.impact ? `<p class="text-xs text-gray-500 mt-1"><em>Impact:</em> ${escapeHtml(info.impact)}</p>` : ''}
                ${info.remediation ? `<p class="text-xs text-gray-500 mt-1"><em>What to do:</em> ${escapeHtml(concreteRemediation(info.remediation, issue))}</p>` : ''}
            </div>
        `;
    }).join('') + (cmds.length ? `
        <div class="border border-gray-200 rounded-md p-3 bg-white">
            <div class="text-sm font-medium mb-1">Commands to remove it</div>
            <p class="text-xs text-gray-500 mb-1">The controller will not run these. Confirm the backup is not needed first.</p>
            ${cmds.map(c => `<code class="reconcile-remediation">${escapeHtml(c)}</code>`).join('')}
        </div>
    ` : '');
}

function reasonInfo(code) {
    return (reasonCatalog && reasonCatalog[code]) || {
        code, label: code, explanation: '', impact: '', remediation: '',
    };
}

// The latest report, cached so the All tab and the detail modal can read orphan
// findings without re-fetching for every row.
let reconcileReport = null;

async function fetchReconcileReport(instanceId, { force = false } = {}) {
    if (reconcileReport && reconcileReport._instanceId === instanceId && !force) {
        return reconcileReport;
    }
    await getReasonCatalog();
    try {
        const report = await api.get(`api/camundas/${instanceId}/backups/reconcile`);
        report._instanceId = instanceId;
        reconcileReport = report;
        return report;
    } catch (err) {
        if (err.status === 404) {
            reconcileReport = null;   // no sweep has run yet
            return null;
        }
        throw err;
    }
}

// Orphans have no history record, so a backup-shaped row is synthesised from the
// finding. Only the backup ID and its timestamp are real; everything the history
// record would have supplied is genuinely unknown and left null for the table to
// render as an em dash.
function orphanToBackupRow(issue) {
    return {
        backup_id: issue.backup_id,
        start_time: issue.backup_time || null,
        end_time: null,
        duration_seconds: null,
        status: 'ORPHANED',
        trigger_type: null,
        backup_stats: null,
        _orphan: issue,
    };
}

// Attaches any reconciliation findings to backups that do have a history record,
// so a COMPLETED backup whose data has actually gone is not shown as simply fine.
function annotateBackupIssues(rows) {
    const issues = reconcileReport?.backup_issues || [];
    if (!issues.length) return;
    const byId = new Map(issues.filter(i => i.tracked).map(i => [i.backup_id, i]));
    rows.forEach(r => {
        if (r.status !== 'ORPHANED' && byId.has(r.backup_id)) {
            r._issue = byId.get(r.backup_id);
        }
    });
}

async function loadOrphanRows(instanceId) {
    try {
        const report = await fetchReconcileReport(instanceId);
        if (!report) return [];
        return (report.backup_issues || [])
            .filter(i => !i.tracked)
            .map(orphanToBackupRow);
    } catch (err) {
        console.warn('Failed to load orphaned backups:', err);
        return [];
    }
}

async function loadOrphanedBackups(instanceId) {
    const el = document.getElementById('backups-table-container');
    if (!el) return;
    el.innerHTML = '<div class="flex justify-center py-8"><div class="spinner"></div></div>';

    let report;
    try {
        report = await fetchReconcileReport(instanceId, { force: true });
    } catch (err) {
        el.innerHTML = `<div class="empty-state"><p>Failed to load orphaned backups</p><p class="text-sm mt-1">${escapeHtml(err.message || '')}</p></div>`;
        return;
    }

    if (!report) {
        renderReconcileNeverRun(instanceId);
        return;
    }

    const rows = (report.backup_issues || []).filter(i => !i.tracked).map(orphanToBackupRow);
    renderBackupsTable(instanceId, rows, { report });
}

// Never having scanned is not the same as having scanned and found nothing.
function renderReconcileNeverRun(instanceId) {
    const el = document.getElementById('backups-table-container');
    if (!el) return;
    el.innerHTML = `
        <div class="empty-state">
            <p class="text-lg font-medium text-gray-600">No scan has run yet</p>
            <p class="text-sm mt-1">Use the refresh button above to compare this instance's backup records against the data that actually exists in Zeebe, the Camunda components and Elasticsearch.</p>
        </div>
    `;
}

// A scan that could not reach every source proves nothing about what it did not
// check, so the header shows per-source state and says when a scan is partial.
function renderReconcileHeader(instanceId, report) {
    if (!report) return '';

    const sources = Object.values(report.sources_checked || {}).sort((a, b) => a.name.localeCompare(b.name));
    const unreachable = sources.filter(s => !s.reachable && !s.skipped).map(s => s.name);

    const chips = sources.map(s => {
        let cls = 'source-chip-ok', mark = '\u2713', title = `${s.count} found`;
        if (s.skipped) {
            cls = 'source-chip-skipped'; mark = '\u2013'; title = 'Not configured for this instance';
        } else if (!s.reachable) {
            cls = 'source-chip-failed'; mark = '\u2717'; title = s.error || 'Could not be checked';
        }
        return `<span class="source-chip ${cls}" title="${escapeHtml(title)}">${mark} ${escapeHtml(s.name)}</span>`;
    }).join(' ');

    const instanceBanners = (report.instance_findings || []).map(f => {
        const info = reasonInfo(f.reason);
        const cls = f.severity === 'critical' ? 'reconcile-banner' : 'reconcile-banner reconcile-banner-warn';
        return `
            <div class="${cls}">
                <div class="flex items-center gap-2 flex-wrap">
                    <span class="severity-badge severity-${escapeHtml(f.severity)}">${escapeHtml(f.severity.replace('_', ' '))}</span>
                    <strong class="text-sm">${escapeHtml(info.label)}</strong>
                    <span class="reason-code">${escapeHtml(f.reason)}</span>
                </div>
                <p class="text-sm mt-1 text-gray-700">${escapeHtml(f.detail || info.explanation)}</p>
                <p class="text-xs mt-1 text-gray-600">${escapeHtml(concreteRemediation(info.remediation, null))}</p>
            </div>
        `;
    }).join('');

    const partial = unreachable.length ? `
        <div class="reconcile-partial">
            <strong>This scan is incomplete.</strong>
            Could not check: ${escapeHtml(unreachable.join(', '))}.
            Anything stored only there is neither confirmed present nor reported missing.
        </div>
    ` : '';

    const repoNote = (report.repository_findings || []).length ? `
        <details class="mt-2 mb-3">
            <summary class="text-sm text-gray-600 cursor-pointer">Other artifacts (${report.repository_findings.length})</summary>
            <div class="mt-2 space-y-2">
                ${report.repository_findings.map(f => {
                    const info = reasonInfo(f.reason);
                    return `
                        <div class="text-sm text-gray-700 pl-3 border-l-2 border-gray-200">
                            <strong>${escapeHtml(info.label)}</strong>
                            <span class="reason-code">${escapeHtml(f.reason)}</span>
                            <p class="text-xs text-gray-600 mt-0.5">${escapeHtml(f.detail || info.explanation)}</p>
                        </div>
                    `;
                }).join('')}
            </div>
        </details>
    ` : '';

    return `
        <div class="mb-4">
            <div class="text-sm text-gray-600 mb-2">Last scanned ${escapeHtml(formatTime(report.finished_at))}</div>
            <div class="flex flex-wrap gap-1 mb-3">${chips}</div>
            ${partial}
            ${instanceBanners}
            ${repoNote}
        </div>
    `;
}

// Findings for a backup that does have a history record. Shown inside the normal
// detail modal, because the record's own status says nothing about whether its
// data still exists.
function renderTrackedBackupFindings(backupId) {
    const issue = (reconcileReport?.backup_issues || []).find(i => i.backup_id === backupId && i.tracked);
    if (!issue) return '';

    const findings = renderFindingCards(issue);

    return `
        <div>
            <h3 class="text-sm font-semibold text-gray-700 mb-2">
                Issues found by the last scan
            </h3>
            <div class="space-y-2">${findings}</div>
        </div>
    `;
}

// Details for an orphan cannot come from the backup history endpoint - there is
// no record to fetch. They come from the cached report instead, rendered in the
// same modal shell as a normal backup.
function showOrphanedBackupDetail(instanceId, backupId) {
    const issue = (reconcileReport?.backup_issues || []).find(i => i.backup_id === backupId);
    if (!issue) {
        showToast('Run a scan to load details for this backup', 'error');
        return;
    }

    const findings = renderFindingCards(issue, { showCommands: true });

    const sourceList = (names, cls, mark) =>
        (names || []).map(n => `<span class="source-chip ${cls}">${mark} ${escapeHtml(n)}</span>`).join(' ');

    const html = `
        <div class="bg-white rounded-xl shadow-xl w-full max-w-3xl">
            <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
                <div>
                    <h2 class="text-lg font-semibold text-gray-900">Orphaned Backup Details</h2>
                    <p class="text-sm text-gray-500 font-mono">${escapeHtml(backupId)}</p>
                </div>
                <button onclick="closeModal()" class="text-gray-400 hover:text-gray-600">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>
            <div class="px-6 py-4 space-y-4 max-h-[70vh] overflow-y-auto">
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                    <div><span class="text-xs text-gray-500 block">Status</span><span class="badge badge-orphaned">ORPHANED</span></div>
                    <div><span class="text-xs text-gray-500 block">Trigger</span><span class="text-sm text-gray-400">—</span></div>
                    <div><span class="text-xs text-gray-500 block">Start</span><span class="text-sm">${issue.backup_time ? escapeHtml(formatTime(issue.backup_time)) : '—'}</span></div>
                    <div><span class="text-xs text-gray-500 block">Duration</span><span class="text-sm text-gray-400">—</span></div>
                </div>

                <div class="bg-yellow-50 border border-yellow-200 rounded-md p-3 text-sm text-gray-700">
                    The controller has no history record for this backup, so its trigger,
                    duration and component results are unknown. The start time is derived
                    from the backup ID.
                </div>

                <div>
                    <h3 class="text-sm font-semibold text-gray-700 mb-2">Where it exists</h3>
                    <div class="flex flex-wrap gap-1">
                        ${sourceList(issue.present_in, 'source-chip-ok', '\u2713') || '<span class="text-sm text-gray-400">—</span>'}
                        ${sourceList(issue.missing_in, 'source-chip-failed', '\u2717')}
                        ${sourceList(issue.unverified, 'source-chip-skipped', '?')}
                    </div>
                </div>

                <div>
                    <h3 class="text-sm font-semibold text-gray-700 mb-2">Findings</h3>
                    <div class="space-y-2">${findings}</div>
                </div>

                ${(issue.implied || []).length ? `
                <div>
                    <h3 class="text-sm font-semibold text-gray-700 mb-2">Also consistent with</h3>
                    <p class="reason-code">${issue.implied.map(c => escapeHtml(c)).join(', ')}</p>
                    <p class="text-xs text-gray-500 mt-1">Explained by the findings above, so not reported separately.</p>
                </div>` : ''}
            </div>
            <div class="px-6 py-4 border-t border-gray-200 flex justify-end">
                <button onclick="closeModal()" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700">
                    Close
                </button>
            </div>
        </div>
    `;
    showModal(html);
}

