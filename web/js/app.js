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
};

// ============================================================
// Initialization
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
    initTabs();
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
        const data = await api.get('/api/status');
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
        const instances = await api.get('/api/camundas');
        state.instances = instances || [];
        const enabled = instances.filter(i => i.enabled);

        if (enabled.length === 0) {
            container.innerHTML = '<p class="text-sm text-gray-500">No enabled instances. <a href="#" onclick="showTab(\'instances\')" class="text-blue-600 hover:underline">Add one</a>.</p>';
            return;
        }

        container.innerHTML = enabled.map(inst => `
            <button onclick="triggerBackup('${escapeForInlineHandler(inst.id)}', '${escapeForInlineHandler(inst.name)}')"
                class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 transition-colors">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"/>
                </svg>
                Backup ${escapeHtml(inst.name)}
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
            const data = await api.get('/api/status');
            state.systemStatus = data;
            updateHeaderStatus(data);

            if (data.active_backups === 0) {
                stopBackupPolling();
                showToast('Backup completed', 'success');
                if (state.currentTab === 'dashboard') loadDashboard();
            }
        } catch (_) { /* ignore polling errors */ }
    }, BACKUP_POLLING_INTERVAL_MS);
}

function stopBackupPolling() {
    if (state.pollingIntervalId) {
        clearInterval(state.pollingIntervalId);
        state.pollingIntervalId = null;
    }
}

// ============================================================
// Trigger Backup
// ============================================================
async function triggerBackup(instanceId, instanceName) {
    try {
        const data = await api.post(`/api/camundas/${instanceId}/backup`);
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
        const instances = await api.get('/api/camundas');
        state.instances = instances || [];
        renderInstancesTable(instances);
    } catch (err) {
        el.innerHTML = `<div class="empty-state"><p>Failed to load instances</p><p class="text-sm mt-1">${err.message}</p></div>`;
    }
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
                        <th>Status</th>
                        <th>Schedule</th>
                        <th class="hidden lg:table-cell">Last Backup</th>
                        <th class="hidden lg:table-cell">Last Status</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${instances.map(inst => `
                        <tr>
                            <td class="font-medium text-gray-900">${escapeHtml(inst.name)}</td>
                            <td class="text-gray-500 text-xs max-w-[200px] truncate">${escapeHtml(inst.base_url)}</td>
                            <td><span class="badge ${inst.enabled ? 'badge-enabled' : 'badge-disabled'}">${inst.enabled ? 'Enabled' : 'Disabled'}</span></td>
                            <td class="text-xs font-mono text-gray-500">${escapeHtml(inst.schedule || '—')}</td>
                            <td class="hidden lg:table-cell text-xs text-gray-500">${inst.last_backup_at ? formatTime(inst.last_backup_at) : '—'}</td>
                            <td class="hidden lg:table-cell">${inst.last_backup_status ? `<span class="badge badge-${inst.last_backup_status.toLowerCase()}">${inst.last_backup_status}</span>` : '—'}</td>
                            <td>
                                <div class="flex items-center gap-1">
                                    <button onclick="openInstanceForm(${escapeAttr(JSON.stringify(inst))})" title="Edit"
                                        class="p-1 text-gray-400 hover:text-blue-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/></svg>
                                    </button>
                                    <button onclick="toggleInstance('${escapeForInlineHandler(inst.id)}', ${!inst.enabled})" title="${inst.enabled ? 'Disable' : 'Enable'}"
                                        class="p-1 text-gray-400 hover:text-yellow-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="${inst.enabled ? 'M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636' : 'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z'}"/></svg>
                                    </button>
                                    <button onclick="confirmDeleteInstance('${escapeForInlineHandler(inst.id)}', '${escapeForInlineHandler(inst.name)}')" title="Delete"
                                        class="p-1 text-gray-400 hover:text-red-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                                    </button>
                                    ${inst.enabled ? `
                                    <button onclick="triggerBackup('${escapeForInlineHandler(inst.id)}', '${escapeForInlineHandler(inst.name)}')" title="Trigger Backup"
                                        class="p-1 text-gray-400 hover:text-green-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"/></svg>
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
// Instance Form (Modal)
// ============================================================
function openInstanceForm(instance) {
    const isEdit = !!instance;
    const inst = instance || {};

    const components = inst.components || [
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
            <form id="instance-form" onsubmit="saveInstance(event, ${isEdit ? `'${escapeForInlineHandler(inst.id)}'` : 'null'})" class="px-6 py-4 space-y-4 max-h-[70vh] overflow-y-auto">
                
                <!-- Basic Info (expanded) -->
                <div class="border border-gray-200 rounded-lg">
                    <button type="button" class="accordion-toggle open w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-700" onclick="toggleAccordion(this)">
                        Basic Info
                        <svg class="accordion-icon w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </button>
                    <div class="accordion-content open px-4 pb-4 space-y-3">
                        ${!isEdit ? `
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">ID <span class="text-red-500">*</span></label>
                            <input type="text" name="id" value="${escapeAttr(inst.id || '')}" required
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="my-camunda-instance">
                        </div>` : ''}
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Name <span class="text-red-500">*</span></label>
                            <input type="text" name="name" value="${escapeAttr(inst.name || '')}" required
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Production Camunda">
                        </div>
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">Base URL <span class="text-red-500">*</span></label>
                            <input type="url" name="base_url" value="${escapeAttr(inst.base_url || '')}" required
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="https://camunda.example.com">
                        </div>
                        <div class="grid grid-cols-2 gap-3">
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Schedule (cron)</label>
                                <input type="text" name="schedule" value="${escapeAttr(inst.schedule || '0 2 * * *')}"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    placeholder="0 2 * * *">
                            </div>
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Retention Count</label>
                                <input type="number" name="retention_count" value="${inst.retention_count || 7}" min="1"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                            </div>
                        </div>
                        <div class="grid grid-cols-2 gap-3">
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Success History</label>
                                <input type="number" name="success_history_count" value="${inst.success_history_count || 30}" min="1"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                            </div>
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">Failure History</label>
                                <input type="number" name="failure_history_count" value="${inst.failure_history_count || 30}" min="1"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                            </div>
                        </div>
                        <div class="flex items-center gap-4">
                            <label class="flex items-center gap-2 text-sm">
                                <input type="checkbox" name="enabled" ${inst.enabled !== false ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
                                Enabled
                            </label>
                            <label class="flex items-center gap-2 text-sm">
                                <input type="checkbox" name="parallel_execution" ${inst.parallel_execution ? 'checked' : ''} class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
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
                        ${['zeebe', 'operate', 'tasklist', 'optimize'].map(comp => {
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
                                <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
                                    <input type="text" name="${comp}_backup_endpoint" value="${escapeAttr(inst[comp + '_backup_endpoint'] || '')}"
                                        class="w-full px-2 py-1.5 border border-gray-300 rounded text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
                                        placeholder="Backup endpoint">
                                    <input type="text" name="${comp}_status_endpoint" value="${escapeAttr(inst[comp + '_status_endpoint'] || '')}"
                                        class="w-full px-2 py-1.5 border border-gray-300 rounded text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
                                        placeholder="Status endpoint">
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
                            <label class="block text-xs font-medium text-gray-700 mb-1">Endpoint URL</label>
                            <input type="text" name="elasticsearch_endpoint" value="${escapeAttr(inst.elasticsearch_endpoint || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="https://elasticsearch.example.com:9200">
                        </div>
                        <div>
                            <label class="block text-xs font-medium text-gray-700 mb-1">Username</label>
                            <input type="text" name="elasticsearch_username" value="${escapeAttr(inst.elasticsearch_username || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="elastic">
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
                            <label class="block text-xs font-medium text-gray-700 mb-1">S3 Endpoint</label>
                            <input type="text" name="s3_endpoint" value="${escapeAttr(inst.s3_endpoint || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="https://s3.amazonaws.com">
                        </div>
                        <div>
                            <label class="block text-xs font-medium text-gray-700 mb-1">Access Key</label>
                            <input type="text" name="s3_accesskey" value="${escapeAttr(inst.s3_accesskey || '')}"
                                class="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="AKIAIOSFODNN7EXAMPLE">
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

    const components = ['zeebe', 'operate', 'tasklist', 'optimize', 'elasticsearch'].map(name => ({
        name,
        enabled: fd.get(`component_${name}_enabled`) === 'on',
    }));

    const payload = {
        id: editId || fd.get('id'),
        name: fd.get('name'),
        base_url: fd.get('base_url'),
        enabled: fd.get('enabled') === 'on',
        schedule: fd.get('schedule'),
        retention_count: parseInt(fd.get('retention_count')) || 7,
        success_history_count: parseInt(fd.get('success_history_count')) || 30,
        failure_history_count: parseInt(fd.get('failure_history_count')) || 30,
        parallel_execution: fd.get('parallel_execution') === 'on',
        components,
        zeebe_backup_endpoint: fd.get('zeebe_backup_endpoint') || '',
        zeebe_status_endpoint: fd.get('zeebe_status_endpoint') || '',
        operate_backup_endpoint: fd.get('operate_backup_endpoint') || '',
        operate_status_endpoint: fd.get('operate_status_endpoint') || '',
        tasklist_backup_endpoint: fd.get('tasklist_backup_endpoint') || '',
        tasklist_status_endpoint: fd.get('tasklist_status_endpoint') || '',
        optimize_backup_endpoint: fd.get('optimize_backup_endpoint') || '',
        optimize_status_endpoint: fd.get('optimize_status_endpoint') || '',
        elasticsearch_endpoint: fd.get('elasticsearch_endpoint') || '',
        elasticsearch_username: fd.get('elasticsearch_username') || '',
        s3_endpoint: fd.get('s3_endpoint') || '',
        s3_accesskey: fd.get('s3_accesskey') || '',
    };

    try {
        if (editId) {
            await api.put(`/api/camundas/${editId}`, payload);
            showToast('Instance updated successfully', 'success');
        } else {
            await api.post('/api/camundas', payload);
            showToast('Instance created successfully', 'success');
        }
        closeModal();
        loadInstances();
    } catch (err) {
        showToast(err.message || 'Failed to save instance', 'error');
    }
}

async function toggleInstance(id, enable) {
    try {
        await api.post(`/api/camundas/${id}/${enable ? 'enable' : 'disable'}`);
        showToast(`Instance ${enable ? 'enabled' : 'disabled'}`, 'success');
        loadInstances();
    } catch (err) {
        showToast(err.message || 'Failed to update instance', 'error');
    }
}

async function confirmDeleteInstance(id, name) {
    const confirmed = await showConfirm(`Are you sure you want to delete "${name}"? This action cannot be undone.`);
    if (!confirmed) return;

    try {
        await api.del(`/api/camundas/${id}`);
        showToast('Instance deleted', 'success');
        loadInstances();
    } catch (err) {
        showToast(err.message || 'Failed to delete instance', 'error');
    }
}

// ============================================================
// Backups Tab
// ============================================================
async function loadBackupsTab() {
    // Load instance selector
    try {
        const instances = await api.get('/api/camundas');
        state.instances = instances || [];
        renderBackupsTabControls(instances);

        if (instances.length > 0) {
            const sel = state.selectedInstanceId || instances[0].id;
            state.selectedInstanceId = sel;
            document.getElementById('backup-instance-select').value = sel;
            loadBackups(sel, state.backupFilter);
        }
    } catch (err) {
        const el = document.getElementById('backups-content');
        if (el) el.innerHTML = `<div class="empty-state"><p>Failed to load instances</p></div>`;
    }
}

function renderBackupsTabControls(instances) {
    const controls = document.getElementById('backups-controls');
    if (!controls) return;

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
        case 'completed': path = `/api/camundas/${instanceId}/backups?status=COMPLETED`; break;
        case 'failed': path = `/api/camundas/${instanceId}/backups/failed`; break;
        case 'incomplete': path = `/api/camundas/${instanceId}/backups/incomplete`; break;
        case 'orphaned': path = `/api/camundas/${instanceId}/backups/orphaned`; break;
        default: path = `/api/camundas/${instanceId}/backups`;
    }

    try {
        const backups = await api.get(path);
        renderBackupsTable(instanceId, backups || []);
    } catch (err) {
        el.innerHTML = `<div class="empty-state"><p>Failed to load backups</p><p class="text-sm mt-1">${err.message}</p></div>`;
    }
}

function renderBackupsTable(instanceId, backups) {
    const el = document.getElementById('backups-table-container');
    if (!el) return;

    if (backups.length === 0) {
        el.innerHTML = `
            <div class="empty-state">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"/></svg>
                <p class="text-lg font-medium text-gray-600">No Backups Found</p>
                <p class="text-sm mt-1">No backups match the current filter.</p>
            </div>
        `;
        return;
    }

    el.innerHTML = `
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
                            <td class="text-xs text-gray-500">${formatTime(b.start_time)}</td>
                            <td class="hidden sm:table-cell text-xs text-gray-500">${b.end_time ? formatTime(b.end_time) : '—'}</td>
                            <td class="hidden md:table-cell text-xs text-gray-500">${b.duration_seconds != null ? formatDuration(b.duration_seconds) : '—'}</td>
                            <td><span class="badge badge-${b.status.toLowerCase()}">${b.status}</span></td>
                            <td class="hidden lg:table-cell"><span class="badge badge-${b.trigger_type.toLowerCase()}">${b.trigger_type}</span></td>
                            <td class="hidden md:table-cell text-xs text-gray-500">
                                ${b.backup_stats ? `<span class="text-green-600">${b.backup_stats.successful_components}</span>/<span class="text-red-600">${b.backup_stats.failed_components}</span>/<span>${b.backup_stats.total_components}</span>` : '—'}
                            </td>
                            <td>
                                <div class="flex items-center gap-1">
                                    <button onclick="showBackupDetail('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(b.backup_id)}')" title="View Details"
                                        class="p-1 text-gray-400 hover:text-blue-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/></svg>
                                    </button>
                                    <button onclick="viewBackupLogs('${escapeForInlineHandler(instanceId)}', '${escapeForInlineHandler(b.backup_id)}')" title="View Logs"
                                        class="p-1 text-gray-400 hover:text-green-600 transition-colors">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
                                    </button>
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
        const backup = await api.get(`/api/camundas/${instanceId}/backups/${backupId}`);
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
                                        <div>Duration: ${comp.duration_seconds ? formatDuration(comp.duration_seconds) : '—'}</div>
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
        const logs = await api.get(`/api/camundas/${instanceId}/backups/${backupId}/logs`);
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
        await api.del(`/api/camundas/${instanceId}/backups/${backupId}`);
        showToast('Backup deleted', 'success');
        loadBackups(instanceId, state.backupFilter);
    } catch (err) {
        showToast(err.message || 'Failed to delete backup', 'error');
    }
}

// ============================================================
// Modal
// ============================================================
function showModal(contentHtml) {
    const backdrop = document.getElementById('modal-backdrop');
    const panel = document.getElementById('modal-panel');
    if (!backdrop || !panel) return;

    panel.innerHTML = contentHtml;

    // Trigger reflow then add active class
    requestAnimationFrame(() => {
        backdrop.classList.add('active');
        panel.classList.add('active');
    });
}

function closeModal() {
    const backdrop = document.getElementById('modal-backdrop');
    const panel = document.getElementById('modal-panel');
    if (!backdrop || !panel) return;

    backdrop.classList.remove('active');
    panel.classList.remove('active');

    setTimeout(() => { panel.innerHTML = ''; }, MODAL_TRANSITION_MS);
}

// Close modal on backdrop click
document.addEventListener('click', (e) => {
    if (e.target.id === 'modal-backdrop') closeModal();
});

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
});

// ============================================================
// Confirm Dialog
// ============================================================
function showConfirm(message) {
    return new Promise((resolve) => {
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
                        <button onclick="window._confirmResolve(false)" class="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
                        <button onclick="window._confirmResolve(true)" class="px-3 py-1.5 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700">Delete</button>
                    </div>
                </div>
            </div>
        `;

        window._confirmResolve = (result) => {
            delete window._confirmResolve;
            closeModal();
            resolve(result);
        };

        showModal(html);
    });
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

function formatTime(isoString) {
    if (!isoString) return '—';
    try {
        const d = new Date(isoString);
        return d.toLocaleString(undefined, {
            month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
        });
    } catch {
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
