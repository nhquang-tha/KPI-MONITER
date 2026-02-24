import os
import jinja2
import pandas as pd
import json
import gc
import re
import zipfile
import unicodedata
import random
import math
import requests
import urllib.parse
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect, or_, and_
from itertools import zip_longest
from collections import defaultdict

# ==============================================================================
# 1. APP CONFIGURATION & DATABASE SETUP
# ==============================================================================

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 280, 'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8684244776:AAEjz9Lv8Zc5u-o6BJoHM3eCGXDBQE6hRUU')

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==============================================================================
# 2. UTILS
# ==============================================================================

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    s = ''
    for c in input_str:
        if c in s1: s += s0[s1.index(c)]
        else: s += c
    return s

def clean_header(col_name):
    col_name = str(col_name).strip()
    special_map = {
        'ENodeBID': 'enodeb_id', 'gNodeB ID': 'gnodeb_id', 'GNODEB_ID': 'gnodeb_id',
        'CELL_ID': 'cell_id', 'SITE_NAME': 'site_name', 'CELL_NAME': 'cell_name',
        'Frenquency': 'frequency', 'Frequency': 'frequency',
        'PCI': 'pci', 'TAC': 'tac', 'MIMO': 'mimo',
        'UL Traffic Volume (GB)': 'ul_traffic_volume_gb',
        'DL Traffic Volume (GB)': 'dl_traffic_volume_gb',
        'Total Data Traffic Volume (GB)': 'traffic',
        'Cell Uplink Average Throughput': 'cell_uplink_average_throughput',
        'Cell Downlink Average Throughput': 'cell_downlink_average_throughput',
        'A User Downlink Average Throughput': 'user_dl_avg_throughput',
        'Cell avaibility rate': 'cell_avaibility_rate',
        'SgNB Addition Success Rate': 'sgnb_addition_success_rate',
        'SgNB Abnormal Release Rate': 'sgnb_abnormal_release_rate',
        'CQI_5G': 'cqi_5g', 'CQI_4G': 'cqi_4g',
        'POI': 'poi_name', 'Cell_Code': 'cell_code', 'Site_Code': 'site_code',
        'CSHT_code': 'csht_code', 'Hãng_SX': 'hang_sx', 'Antena': 'antena',
        'Swap': 'swap', 'Start_day': 'start_day', 'Ghi_chú': 'ghi_chu',
        'Anten_height': 'anten_height', 'Azimuth': 'azimuth', 'M_T': 'm_t', 'E_T': 'e_t', 'Total_tilt': 'total_tilt',
        'PSC': 'psc', 'DL_UARFCN': 'dl_uarfcn', 'BSC_LAC': 'bsc_lac', 'CI': 'ci',
        'Latitude': 'latitude', 'Longitude': 'longitude', 'Equipment': 'equipment',
        'nrarfcn': 'nrarfcn', 'Lcrid': 'lcrid', 'Đồng_bộ': 'dong_bo',
        'CellID': 'cellid', 'NetworkTech': 'networktech'
    }
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper: return val
    clean = re.sub(r'[^a-z0-9]', '_', remove_accents(col_name).lower())
    clean = re.sub(r'_+', '_', clean)
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo',
        'ten_cell': 'ten_cell', 'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap',
        'traffic_vol_dl': 'traffic_vol_dl', 'res_blk_dl': 'res_blk_dl',
        'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges',
        'cs_so_att': 'cs_so_att', 'ps_so_att': 'ps_so_att',
        'service_drop_all': 'service_drop_all', 'user_dl_avg_thput': 'user_dl_avg_thput',
        'poi': 'poi_name', 'cell_code': 'cell_code', 'site_code': 'site_code'
    }
    return common_map.get(clean, clean)

def generate_colors(n):
    base = ['#0078d4', '#107c10', '#d13438', '#ffaa44', '#00bcf2', '#5c2d91', '#e3008c', '#b4009e']
    if n <= len(base): return base[:n]
    return base + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base))]

# ==============================================================================
# 3. MODELS
# ==============================================================================

class User(UserMixin, db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)

class RF3G(db.Model):
    __tablename__ = 'rf_3g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    bsc_lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    hang_sx = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF4G(db.Model):
    __tablename__ = 'rf_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF5G(db.Model):
    __tablename__ = 'rf_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    site_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    nrarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    dong_bo = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class POI4G(db.Model):
    __tablename__ = 'poi_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200), index=True)

class POI5G(db.Model):
    __tablename__ = 'poi_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200), index=True)

class KPI3G(db.Model):
    __tablename__ = 'kpi_3g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    pstraffic = db.Column(db.Float)
    cssr = db.Column(db.Float)
    dcr = db.Column(db.Float)
    ps_cssr = db.Column(db.Float)
    ps_dcr = db.Column(db.Float)
    hsdpa_throughput = db.Column(db.Float)
    hsupa_throughput = db.Column(db.Float)
    cs_so_att = db.Column(db.Float)
    ps_so_att = db.Column(db.Float)
    csconges = db.Column(db.Float)
    psconges = db.Column(db.Float)
    stt = db.Column(db.String(50))
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))

class KPI4G(db.Model):
    __tablename__ = 'kpi_4g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    traffic_vol_dl = db.Column(db.Float)
    traffic_vol_ul = db.Column(db.Float)
    cell_dl_avg_thputs = db.Column(db.Float)
    cell_ul_avg_thput = db.Column(db.Float)
    user_dl_avg_thput = db.Column(db.Float)
    user_ul_avg_thput = db.Column(db.Float)
    erab_ssrate_all = db.Column(db.Float)
    service_drop_all = db.Column(db.Float)
    unvailable = db.Column(db.Float)
    res_blk_dl = db.Column(db.Float)
    cqi_4g = db.Column(db.Float)
    stt = db.Column(db.String(50))
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))

class KPI5G(db.Model):
    __tablename__ = 'kpi_5g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    dl_traffic_volume_gb = db.Column(db.Float)
    ul_traffic_volume_gb = db.Column(db.Float)
    cell_downlink_average_throughput = db.Column(db.Float)
    cell_uplink_average_throughput = db.Column(db.Float)
    user_dl_avg_throughput = db.Column(db.Float)
    cqi_5g = db.Column(db.Float)
    cell_avaibility_rate = db.Column(db.Float)
    sgnb_addition_success_rate = db.Column(db.Float)
    sgnb_abnormal_release_rate = db.Column(db.Float)
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_gnodeb = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))

class QoE4G(db.Model):
    __tablename__ = 'qoe_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_name = db.Column(db.String(100), index=True)
    week_name = db.Column(db.String(100))
    qoe_score = db.Column(db.Float)
    qoe_percent = db.Column(db.Float)
    details = db.Column(db.Text)

class QoS4G(db.Model):
    __tablename__ = 'qos_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_name = db.Column(db.String(100), index=True)
    week_name = db.Column(db.String(100))
    qos_score = db.Column(db.Float)
    qos_percent = db.Column(db.Float)
    details = db.Column(db.Text)

class ITSLog(db.Model):
    __tablename__ = 'its_log'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    networktech = db.Column(db.String(20))
    level = db.Column(db.Float)
    qual = db.Column(db.Float)
    cellid = db.Column(db.String(50))

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin123')
            db.session.add(u)
            db.session.commit()
init_database()

# ==============================================================================
# 4. TEMPLATES (DEFINED AT MODULE LEVEL - DO NOT INDENT)
# ==============================================================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor System</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
    <link href='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.css' rel='stylesheet' />
    <script src='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.js'></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --acrylic-blur: blur(20px); --primary-color: #0078d4; --text-color: #212529; --shadow-soft: 0 4px 12px rgba(0,0,0,0.05); }
        body { background: linear-gradient(135deg, #f3f4f6 0%, #eef2f3 100%); font-family: 'Segoe UI', sans-serif; color: var(--text-color); overflow-x: hidden; }
        .sidebar { height: 100vh; width: 260px; position: fixed; top: 0; left: 0; background: rgba(240, 240, 245, 0.85); backdrop-filter: var(--acrylic-blur); z-index: 1000; transition: all 0.3s; padding-top: 1rem; overflow-y: auto; }
        .sidebar-header { padding: 1.5rem; color: var(--primary-color); font-weight: 600; font-size: 1.5rem; text-align: center; }
        .sidebar-menu { padding: 0; list-style: none; margin: 1rem 0; }
        .sidebar-menu a { display: flex; align-items: center; padding: 14px 25px; color: #555; text-decoration: none; font-weight: 500; border-left: 4px solid transparent; margin: 4px 12px; border-radius: 8px; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background-color: rgba(255,255,255,0.95); color: var(--primary-color); border-left-color: var(--primary-color); }
        .sidebar-menu i { margin-right: 15px; width: 24px; text-align: center; }
        .main-content { margin-left: 260px; padding: 30px; min-height: 100vh; }
        .card { border: none; border-radius: 12px; background: rgba(255,255,255,0.85); box-shadow: var(--shadow-soft); margin-bottom: 1.5rem; }
        .chart-container canvas { cursor: zoom-in; }
        @media (max-width: 768px) { .sidebar { margin-left: -260px; } .main-content { margin-left: 0; padding: 15px; } }
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired"></i> NetOps</div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/gis" class="{{ 'active' if active_page == 'gis' else '' }}"><i class="fa-solid fa-map-location-dot"></i> Bản đồ GIS</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="/qoe-qos" class="{{ 'active' if active_page == 'qoe_qos' else '' }}"><i class="fa-solid fa-star-half-stroke"></i> QoE QoS Analytics</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-pin"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}"><i class="fa-solid fa-code"></i> Script</a></li>
            {% if current_user.role == 'admin' %}
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Data Import</a></li>
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
            {% endif %}
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Profile</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Logout</a></li>
        </ul>
    </div>

    <div class="main-content">
        <button class="btn btn-light shadow-sm d-md-none mb-3 border" onclick="document.getElementById('sidebar').classList.toggle('active')"><i class="fa-solid fa-bars"></i> Menu</button>
        <div class="container-fluid p-0">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}{% for category, message in messages %}<div class="alert alert-{{ category }} alert-dismissible fade show shadow-sm border-0 mb-4">{{ message }}<button type="button" class="btn-close" data-bs-dismiss="alert"></button></div>{% endfor %}{% endif %}
            {% endwith %}
            {% block content %}{% endblock %}
        </div>
    </div>
    
    <div class="modal fade" id="chartDetailModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-xl modal-dialog-centered">
            <div class="modal-content border-0 shadow-lg" style="background: rgba(255,255,255,0.95); backdrop-filter: blur(15px);">
                <div class="modal-header border-0 pb-0">
                    <h5 class="modal-title text-primary fw-bold" id="modalTitle"></h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body p-4"><div class="chart-container" style="position: relative; height:65vh; width:100%"><canvas id="modalChart"></canvas></div></div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        let modalChartInstance = null;
        function showDetailModal(cellName, date, value, metricLabel, allDatasets, allLabels) {
            document.getElementById('modalTitle').innerText = 'Chi tiết ' + metricLabel;
            const ctx = document.getElementById('modalChart').getContext('2d');
            if (modalChartInstance) modalChartInstance.destroy();
            modalChartInstance = new Chart(ctx, { type: 'line', data: { labels: allLabels, datasets: allDatasets }, options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false }, plugins: { legend: { display: true } }, spanGaps: true, elements: { line: { tension: 0.3 } } } });
            new bootstrap.Modal(document.getElementById('chartDetailModal')).show();
        }
        function toggleCheckboxes(source) {
            let checkboxes = document.getElementsByName('tables');
            for(var i=0; i<checkboxes.length; i++) checkboxes[i].checked = source.checked;
        }
        function addRow(tech) {
            var table = document.getElementById("rruTable_" + tech).getElementsByTagName('tbody')[0];
            var newRow = table.insertRow(table.rows.length);
            var idx = table.rows.length;
            var defaultSRN, defaultSlot;
            if (tech == '3g900') { defaultSRN = 70 + idx - 1; defaultSlot = 2; }
            else if (tech == '3g2100') { defaultSRN = 80 + idx - 1; defaultSlot = 3; }
            else { defaultSRN = 60 + idx - 1; defaultSlot = 3; }
            newRow.insertCell(0).innerHTML = `<input type="text" name="rn[]" class="form-control" value="RRU${idx}">`;
            newRow.insertCell(1).innerHTML = `<input type="number" name="srn[]" class="form-control" value="${defaultSRN}">`;
            newRow.insertCell(2).innerHTML = `<input type="number" name="hsn[]" class="form-control" value="${defaultSlot}">`;
            newRow.insertCell(3).innerHTML = `<input type="number" name="hpn[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(4).innerHTML = `<input type="number" name="rcn[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(5).innerHTML = `<input type="number" name="sectorid[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(6).innerHTML = `<input type="number" name="rxnum[]" class="form-control" value="${tech=='4g'?4:2}">`;
            newRow.insertCell(7).innerHTML = `<input type="number" name="txnum[]" class="form-control" value="${tech=='4g'?4:1}">`;
            newRow.insertCell(8).innerHTML = `<button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button>`;
        }
    </script>
</body>
</html>
"""

LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="vi">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Đăng nhập | NetOps</title><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body { background: linear-gradient(135deg, #f0f2f5 0%, #d9e2ec 100%); height: 100vh; display: flex; align-items: center; justify-content: center; font-family: 'Segoe UI', sans-serif; } .login-card { width: 100%; max-width: 400px; background: rgba(255, 255, 255, 0.9); backdrop-filter: blur(20px); padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); } .btn-primary { background-color: #0078d4; border: none; padding: 10px; font-weight: 600; border-radius: 8px; width: 100%; } .form-control { border-radius: 8px; padding: 12px; margin-bottom: 1rem; }</style>
</head>
<body>
    <div class="login-card"><h3 class="text-center mb-4 text-primary fw-bold">Welcome Back</h3>
        <form method="POST"><label class="form-label fw-bold small">USERNAME</label><input type="text" name="username" class="form-control" required><label class="form-label fw-bold small">PASSWORD</label><input type="password" name="password" class="form-control" required><button type="submit" class="btn btn-primary">Sign In</button></form>
    </div>
</body>
</html>
"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm mb-4">
    <div class="card-header d-flex justify-content-between align-items-center bg-white border-bottom">
        <span class="fs-5 fw-bold text-secondary">{{ title }}</span>
        <span class="badge bg-primary px-3 py-2 rounded-pill">{{ current_user.role | upper }}</span>
    </div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            {% if dashboard_data and dashboard_data.labels %}
                <div class="row g-4">
                    <div class="col-md-6"><div class="card border shadow-sm"><div class="card-body"><h6 class="fw-bold text-primary mb-3">Tổng Traffic 4G (GB)</h6><div style="height:30vh; width:100%"><canvas id="chartTraffic"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border shadow-sm"><div class="card-body"><h6 class="fw-bold text-success mb-3">Trung bình User DL Thput (Mbps)</h6><div style="height:30vh; width:100%"><canvas id="chartThput"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border shadow-sm"><div class="card-body"><h6 class="fw-bold text-warning mb-3">Trung bình Tài nguyên PRB DL (%)</h6><div style="height:30vh; width:100%"><canvas id="chartPrb"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border shadow-sm"><div class="card-body"><h6 class="fw-bold text-info mb-3">Trung bình CQI 4G</h6><div style="height:30vh; width:100%"><canvas id="chartCqi"></canvas></div></div></div></div>
                </div>
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        const labels = {{ dashboard_data.labels | tojson }};
                        function createDashChart(id, label, color, bgColor, dataArr, titleStr) {
                            const ds = [{ label: label, data: dataArr, borderColor: color, backgroundColor: bgColor, fill: true, tension: 0.3, borderWidth: 2 }];
                            new Chart(document.getElementById(id).getContext('2d'), { type: 'line', data: { labels: labels, datasets: ds }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, onClick: (e, el) => { if (el.length > 0) showDetailModal(ds[0].label, labels[el[0].index], ds[0].data[el[0].index], titleStr, ds, labels); } } });
                        }
                        createDashChart('chartTraffic', 'Traffic (GB)', '#0078d4', 'rgba(0,120,212,0.1)', {{ dashboard_data.traffic | tojson }}, 'Tổng Traffic 4G');
                        createDashChart('chartThput', 'Avg Thput (Mbps)', '#107c10', 'rgba(16,124,16,0.1)', {{ dashboard_data.thput | tojson }}, 'Trung bình Thput');
                        createDashChart('chartPrb', 'Avg PRB (%)', '#ffaa44', 'rgba(255,170,68,0.1)', {{ dashboard_data.prb | tojson }}, 'Trung bình PRB');
                        createDashChart('chartCqi', 'Avg CQI', '#00bcf2', 'rgba(0,188,242,0.1)', {{ dashboard_data.cqi | tojson }}, 'Trung bình CQI');
                    });
                </script>
            {% else %}
                <div class="alert alert-info border-0 shadow-sm">Chưa có dữ liệu KPI 4G.</div>
            {% endif %}
        
        {% elif active_page == 'gis' %}
            <form method="POST" action="/gis" enctype="multipart/form-data" class="row g-3 bg-light p-3 mb-4 rounded border">
                <div class="col-md-2"><label class="form-label small">CÔNG NGHỆ</label><select name="tech" class="form-select"><option value="3g" {% if selected_tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech=='5g' %}selected{% endif %}>5G</option></select></div>
                <div class="col-md-2"><label class="form-label small">SITE CODE</label><input type="text" name="site_code" class="form-control" value="{{ site_code_input }}"></div>
                <div class="col-md-3"><label class="form-label small">CELL NAME</label><input type="text" name="cell_name" class="form-control" value="{{ cell_name_input }}"></div>
                <div class="col-md-3"><label class="form-label small">LOG ITS (.TXT/.CSV)</label><input type="file" name="its_file" class="form-control" accept=".txt,.csv"></div>
                <div class="col-md-2 d-flex flex-column gap-2 mt-4 pt-1"><button type="submit" name="action" value="search" class="btn btn-primary btn-sm">Tìm kiếm</button><button type="submit" name="action" value="show_log" class="btn btn-warning btn-sm">Xem Log File</button></div>
            </form>
            <div class="card"><div class="card-body p-1"><div id="gisMap" style="height: 65vh; width: 100%; z-index: 1;"></div></div></div>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    var gisData = {{ gis_data | tojson | safe if gis_data else '[]' }};
                    var itsData = {{ its_data | tojson | safe if its_data else '[]' }};
                    if(!document.getElementById('gisMap')) return;
                    var map = L.map('gisMap').setView([19.807, 105.776], 9);
                    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
                    
                    var bounds = [];
                    if (itsData.length > 0) itsData.forEach(p => bounds.push([p.lat, p.lon]));
                    if (gisData.length > 0) gisData.forEach(c => bounds.push([c.lat, c.lon]));
                    if (bounds.length > 0) map.fitBounds(bounds);

                    gisData.forEach(function(cell) {
                        if(!cell.lat || !cell.lon) return;
                        L.circleMarker([cell.lat, cell.lon], {radius: 4, color: '#333', fillColor: '#fff', fillOpacity: 1}).bindPopup("<b>Site:</b> " + cell.site_code).addTo(map);
                        
                        var radMap = (cell.azi || 0) * Math.PI / 180;
                        var dx = (350 * Math.sin(radMap)) / (111320 * Math.cos(cell.lat * Math.PI / 180)); 
                        var dy = (350 * Math.cos(radMap)) / 111320; 
                        var p1 = [cell.lat, cell.lon];
                        var p2 = [cell.lat + dy + dx*0.5, cell.lon + dx - dy*0.5];
                        var p3 = [cell.lat + dy - dx*0.5, cell.lon + dx + dy*0.5];
                        L.polygon([p1, p2, p3], {color: '#0078d4', fillColor: '#0078d4', fillOpacity: 0.35}).bindPopup("<b>Cell:</b> " + cell.cell_name).addTo(map);
                    });

                    itsData.forEach(function(pt) {
                        var color = pt.level >= -85 ? '#4CAF50' : (pt.level >= -105 ? '#FFFF4D' : '#FF4D4D');
                        L.circleMarker([pt.lat, pt.lon], {radius: 3, fillColor: color, color: '#000', weight: 0.5, fillOpacity: 0.9}).bindPopup("Level: " + pt.level).addTo(map);
                    });
                });
            </script>

        {% elif active_page == 'kpi' %}
            <form method="GET" action="/kpi" class="row g-3 bg-light p-3 mb-4 rounded border">
                <div class="col-md-2"><label class="small fw-bold">TECH</label><select name="tech" class="form-select"><option value="3g" {% if selected_tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech=='5g' %}selected{% endif %}>5G</option></select></div>
                <div class="col-md-4"><label class="small fw-bold">POI</label><input type="text" name="poi_name" list="poi_list" class="form-control" value="{{ selected_poi }}"><datalist id="poi_list">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                <div class="col-md-4"><label class="small fw-bold">CELL NAME</label><input type="text" name="cell_name" class="form-control" value="{{ cell_name_input }}"></div>
                <div class="col-md-2 align-self-end"><button class="btn btn-primary w-100">Xem</button></div>
            </form>
            {% if charts %}
                {% for chart_id, c in charts.items() %}
                <div class="card mb-4"><div class="card-body"><h6 class="fw-bold">{{ c.title }}</h6><div style="height:40vh"><canvas id="{{ chart_id }}"></canvas></div></div></div>
                <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,elements:{line:{tension:0.3}},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                {% endfor %}
            {% else %}
                <div class="alert alert-warning">Không có dữ liệu.</div>
            {% endif %}

        {% elif active_page == 'qoe_qos' %}
            <form method="GET" action="/qoe-qos" class="row g-3 bg-light p-3 mb-4 rounded border">
                <div class="col-md-6"><label class="small fw-bold">CELL NAME 4G</label><input type="text" name="cell_name" class="form-control" value="{{ cell_name_input }}" required></div>
                <div class="col-md-2 align-self-end"><button class="btn btn-primary w-100">Tra cứu</button></div>
                {% if has_data %}<div class="col-md-3 align-self-end"><a href="/kpi?tech=4g&cell_name={{ cell_name_input }}" class="btn btn-success w-100">Xem KPI</a></div>{% endif %}
            </form>
            {% if charts %}
                <div class="row">
                    {% for chart_id, c in charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card"><div class="card-body"><h6 class="fw-bold">{{ c.title }}</h6><div style="height:35vh"><canvas id="{{ chart_id }}"></canvas></div></div></div></div>
                    <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,elements:{line:{tension:0.3}},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                    {% endfor %}
                </div>
                {% if qoe_details %}
                <div class="card mb-4"><div class="card-header fw-bold text-primary">Chi tiết QoE</div><div class="card-body p-0 table-responsive"><table class="table table-bordered mb-0 small"><thead><tr><th>Tuần</th>{% for k in qoe_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qoe_details %}<tr><td>{{ row.week }}</td>{% for k in qoe_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
                {% if qos_details %}
                <div class="card mb-4"><div class="card-header fw-bold text-success">Chi tiết QoS</div><div class="card-body p-0 table-responsive"><table class="table table-bordered mb-0 small"><thead><tr><th>Tuần</th>{% for k in qos_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qos_details %}<tr><td>{{ row.week }}</td>{% for k in qos_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
            {% elif cell_name_input %}
                <div class="alert alert-warning">Không có dữ liệu QoE/QoS.</div>
            {% endif %}

        {% elif active_page == 'poi' %}
            <form method="GET" action="/poi" class="row g-3 bg-light p-3 mb-4 rounded border">
                <div class="col-md-8"><label class="small fw-bold">CHỌN POI</label><input type="text" name="poi_name" list="poi_list" class="form-control" value="{{ selected_poi }}"><datalist id="poi_list">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                <div class="col-md-4 align-self-end"><button class="btn btn-primary w-100">Xem</button></div>
            </form>
            {% if poi_charts %}
                <div class="row">
                    {% for chart_id, c in poi_charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card"><div class="card-body"><h6 class="fw-bold">{{ c.title }}</h6><div style="height:35vh"><canvas id="{{ chart_id }}"></canvas></div></div></div></div>
                    <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,elements:{line:{tension:0.3}},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                    {% endfor %}
                </div>
            {% endif %}

        {% elif active_page == 'worst_cell' %}
            <form method="GET" action="/worst-cell" class="row g-3 bg-light p-3 mb-4 rounded border">
                <div class="col-auto"><select name="duration" class="form-select"><option value="1" {% if duration==1 %}selected{% endif %}>1 ngày</option><option value="3" {% if duration==3 %}selected{% endif %}>3 ngày</option><option value="7" {% if duration==7 %}selected{% endif %}>7 ngày</option></select></div>
                <div class="col-auto"><button name="action" value="execute" class="btn btn-danger">Lọc Worst Cell</button></div>
                <div class="col-auto"><button name="action" value="export" class="btn btn-success">Export</button></div>
            </form>
            <div class="table-responsive bg-white rounded border"><table class="table table-hover mb-0"><thead><tr><th>Cell Name</th><th>Avg Thput</th><th>Avg PRB</th><th>Avg CQI</th><th>Avg Drop Rate</th><th>Action</th></tr></thead><tbody>{% for r in worst_cells %}<tr><td>{{ r.cell_name }}</td><td>{{ r.avg_thput }}</td><td>{{ r.avg_res_blk }}</td><td>{{ r.avg_cqi }}</td><td>{{ r.avg_drop }}</td><td><a href="/kpi?tech=4g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success">View</a></td></tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'traffic_down' %}
             <form method="GET" action="/traffic-down" class="row g-3 bg-light p-3 mb-4 rounded border">
                <div class="col-auto"><select name="tech" class="form-select"><option value="3g">3G</option><option value="4g" selected>4G</option></select></div>
                <div class="col-auto"><button name="action" value="execute" class="btn btn-primary">Thực hiện</button></div>
            </form>
            <div class="row g-4">
                <div class="col-md-6"><div class="card"><div class="card-header bg-danger text-white">Zero Traffic</div><div class="card-body p-0 table-responsive"><table class="table mb-0"><thead><tr><th>Cell</th><th>Today</th><th>Avg 7D</th></tr></thead><tbody>{% for r in zero_traffic %}<tr><td>{{ r.cell_name }}</td><td>{{ r.traffic_today }}</td><td>{{ r.avg_last_7 }}</td></tr>{% endfor %}</tbody></table></div></div></div>
                <div class="col-md-6"><div class="card"><div class="card-header bg-warning">Degraded Traffic</div><div class="card-body p-0 table-responsive"><table class="table mb-0"><thead><tr><th>Cell</th><th>Today</th><th>Last Wk</th><th>Degrade</th></tr></thead><tbody>{% for r in degraded %}<tr><td>{{ r.cell_name }}</td><td>{{ r.traffic_today }}</td><td>{{ r.traffic_last_week }}</td><td>-{{ r.degrade_percent }}%</td></tr>{% endfor %}</tbody></table></div></div></div>
            </div>

        {% elif active_page == 'conges_3g' %}
            <form method="GET" action="/conges-3g" class="mb-4"><button name="action" value="execute" class="btn btn-primary">Thực hiện</button></form>
            <div class="table-responsive bg-white rounded border"><table class="table table-hover mb-0"><thead><tr><th>Cell Name</th><th>CS Conges</th><th>PS Conges</th></tr></thead><tbody>{% for r in conges_data %}<tr><td>{{ r.cell_name }}</td><td>{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_conges }}</td></tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'rf' %}
             <div class="mb-4"><a href="/rf?tech=3g" class="btn btn-primary">3G</a> <a href="/rf?tech=4g" class="btn btn-primary">4G</a> <a href="/rf?tech=5g" class="btn btn-primary">5G</a> <a href="/rf/add?tech={{ current_tech }}" class="btn btn-success float-end">Thêm mới</a></div>
             <div class="table-responsive bg-white border" style="max-height: 70vh;"><table class="table table-hover mb-0"><thead><tr><th>Action</th>{% for col in rf_columns %}<th>{{ col }}</th>{% endfor %}</tr></thead><tbody>{% for row in rf_data %}<tr><td><a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-danger">Xóa</a></td>{% for col in rf_columns %}<td>{{ row[col] }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'import' %}
             <div class="row">
                 <div class="col-md-8">
                     <div class="card"><div class="card-body">
                         <ul class="nav nav-tabs mb-4">
                             <li class="nav-item"><a class="nav-link active" data-bs-toggle="tab" href="#tRF">Import RF/KPI/POI</a></li>
                             <li class="nav-item"><a class="nav-link fw-bold text-primary" data-bs-toggle="tab" href="#tQoE">Import QoE/QoS</a></li>
                         </ul>
                         <div class="tab-content">
                             <div class="tab-pane fade show active" id="tRF">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <select name="type" class="form-select mb-3"><option value="rf4g">RF 4G</option><option value="kpi4g">KPI 4G</option><option value="poi4g">POI 4G</option><option value="rf3g">RF 3G</option><option value="kpi3g">KPI 3G</option><option value="rf5g">RF 5G</option><option value="kpi5g">KPI 5G</option></select>
                                     <input type="file" name="file" class="form-control mb-3" multiple required>
                                     <button class="btn btn-primary w-100">Upload</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tQoE">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <select name="type" class="form-select border-primary mb-3"><option value="qoe4g">QoE 4G (Hàng Tuần)</option><option value="qos4g">QoS 4G (Hàng Tuần)</option></select>
                                     <input type="text" name="week_name" class="form-control mb-3" placeholder="VD: Tuần 1 (29/12-04/01)" required>
                                     <input type="file" name="file" class="form-control mb-3" multiple required>
                                     <button class="btn btn-primary w-100">Upload</button>
                                 </form>
                             </div>
                         </div>
                     </div></div>
                 </div>
             </div>
        
        {% elif active_page == 'script' %}
             <div class="card"><div class="card-body">Trình tạo Script chưa khả dụng bản rút gọn. Vui lòng liên hệ Admin.</div></div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

SCRIPT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card"><div class="card-body">Script Generator</div></div>
{% endblock %}
"""

USER_MANAGEMENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row"><div class="col-md-4"><div class="card"><div class="card-header">Add User</div><div class="card-body"><form method="POST" action="/users/add"><input name="username" class="form-control mb-2" required><input name="password" type="password" class="form-control mb-2" required><select name="role" class="form-select mb-3"><option value="user">User</option><option value="admin">Admin</option></select><button class="btn btn-success w-100">Create</button></form></div></div></div><div class="col-md-8"><div class="card"><div class="card-header">Users</div><table class="table"><thead><tr><th>ID</th><th>User</th><th>Role</th><th>Action</th></tr></thead><tbody>{% for u in users %}<tr><td>{{ u.id }}</td><td>{{ u.username }}</td><td>{{ u.role }}</td><td>{% if u.username!='admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-danger">Del</a>{% endif %}</td></tr>{% endfor %}</tbody></table></div></div></div>
{% endblock %}
"""

PROFILE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card"><div class="card-body"><form method="POST" action="/change-password"><input type="password" name="current_password" class="form-control mb-3" placeholder="Current" required><input type="password" name="new_password" class="form-control mb-3" placeholder="New" required><button class="btn btn-primary">Save</button></form></div></div>
{% endblock %}
"""

BACKUP_RESTORE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row"><div class="col-md-6"><div class="card"><div class="card-header">Backup</div><div class="card-body"><form action="/backup" method="POST"><button type="submit" class="btn btn-primary w-100">Download DB</button></form></div></div></div></div>
{% endblock %}
"""

RF_FORM_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card"><div class="card-body"><form method="POST">{% for col in columns %}<div class="mb-2"><label>{{ col }}</label><input type="text" name="{{ col }}" class="form-control" value="{{ obj[col] if obj else '' }}"></div>{% endfor %}<button class="btn btn-primary mt-3">Save</button></form></div></div>
{% endblock %}
"""

RF_DETAIL_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card"><div class="card-body"><table class="table">{% for k,v in obj.items() %}<tr><th>{{ k }}</th><td>{{ v }}</td></tr>{% endfor %}</table></div></div>
{% endblock %}
"""

app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})

def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE: return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

# ==============================================================================
# 5. ROUTES
# ==============================================================================

def send_telegram_message(chat_id, text_content):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text_content, "parse_mode": "HTML"})

def send_telegram_photo(chat_id, photo_url, caption=""):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    requests.post(url, json={"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"})

def process_bot_command(text):
    text = str(text).strip().upper()
    parts = text.split()
    if len(parts) < 3: return "🤖 Lỗi cú pháp!"
        
    cmd = parts[0]
    tech = parts[1].lower()
    target = parts[2]
    
    with app.app_context():
        if cmd == 'KPI':
            Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
            if not Model: return "❌ Lỗi"
            record = Model.query.filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).first()
            if record: return f"📊 {record.ten_cell} - Traffic: {record.traffic}"
            return "❌ Không thấy"
        elif cmd in ['CHART', 'BIEUDO']:
            Model = {'4g': KPI4G}.get(tech)
            if Model:
                records = db.session.query(Model).filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).limit(7).all()
                if records:
                    records.reverse()
                    labels = [r.thoi_gian for r in records if r.thoi_gian]
                    data = [r.traffic or 0 for r in records]
                    chart_config = {"type": "line", "data": {"labels": labels, "datasets": [{"label": "Traffic", "data": data, "borderColor": "blue", "fill": False}]}}
                    encoded = urllib.parse.quote(json.dumps(chart_config))
                    return [{"type": "photo", "url": f"https://quickchart.io/chart?c={encoded}", "caption": "Traffic"}]
    return "Lỗi lệnh"

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    data = request.json
    if data and 'message' in data:
        chat_id = data['message']['chat']['id']
        text = data['message'].get('text', '')
        if text:
            reply_data = process_bot_command(text)
            if isinstance(reply_data, list):
                for item in reply_data:
                    if item.get('type') == 'photo': send_telegram_photo(chat_id, item['url'], item.get('caption', ''))
            elif isinstance(reply_data, dict) and reply_data.get('type') == 'photo':
                send_telegram_photo(chat_id, reply_data['url'], reply_data.get('caption', ''))
            else:
                send_telegram_message(chat_id, str(reply_data))
    return jsonify({"status": "success"}), 200

@app.route('/telegram/set_webhook')
def set_telegram_webhook():
    if not TELEGRAM_BOT_TOKEN: return "Missing Bot Token", 400
    webhook_url = request.host_url.rstrip('/') + url_for('telegram_webhook')
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook?url={webhook_url}"
    try:
        r = requests.get(api_url)
        return jsonify(r.json())
    except Exception as e: return str(e), 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']):
            login_user(user)
            return redirect(url_for('index'))
        flash('Login failed', 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    dashboard_data = {'labels': [], 'traffic': [], 'thput': [], 'prb': [], 'cqi': []}
    try:
        records = db.session.query(KPI4G.thoi_gian, func.sum(KPI4G.traffic).label('traffic'), func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput'), func.avg(KPI4G.res_blk_dl).label('res_blk_dl'), func.avg(KPI4G.cqi_4g).label('cqi_4g')).group_by(KPI4G.thoi_gian).all()
        if records:
            agg_data = {}
            for r in records:
                if not r[0]: continue
                agg_data[r[0]] = {'traffic_sum': r[1] or 0, 'thput_avg': r[2] or 0, 'prb_avg': r[3] or 0, 'cqi_avg': r[4] or 0}
            sorted_dates = sorted(agg_data.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
            dashboard_data['labels'] = sorted_dates
            for d in sorted_dates:
                dashboard_data['traffic'].append(round(agg_data[d]['traffic_sum'], 2))
                dashboard_data['thput'].append(round(agg_data[d]['thput_avg'], 2))
                dashboard_data['prb'].append(round(agg_data[d]['prb_avg'], 2))
                dashboard_data['cqi'].append(round(agg_data[d]['cqi_avg'], 2))
    except Exception as e: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard', dashboard_data=dashboard_data)

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis():
    action_type = request.form.get('action', 'search') if request.method == 'POST' else 'search'
    tech = request.form.get('tech', '4g') if request.method == 'POST' else request.args.get('tech', '4g')
    site_code_input = request.form.get('site_code', '').strip() if request.method == 'POST' else request.args.get('site_code', '').strip()
    cell_name_input = request.form.get('cell_name', '').strip() if request.method == 'POST' else request.args.get('cell_name', '').strip()
    
    show_its = False
    its_data = []
    matched_sites = set()
    gis_data = []

    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    
    if Model:
        query = db.session.query(Model)
        if site_code_input: query = query.filter(Model.site_code.ilike(f"%{site_code_input}%"))
        if cell_name_input: query = query.filter(Model.cell_code.ilike(f"%{cell_name_input}%"))
        if not site_code_input and not cell_name_input: query = query.limit(500)

        records = query.all()
        cols = [c.key for c in Model.__table__.columns if c.key not in ['id']]
        for r in records:
            try:
                lat, lon = float(r.latitude), float(r.longitude)
                azi = int(r.azimuth) if getattr(r, 'azimuth', None) is not None else 0
                if 8 <= lat <= 24 and 102 <= lon <= 110:
                    gis_data.append({
                        'cell_name': getattr(r, 'cell_code', ''),
                        'site_code': r.site_code,
                        'lat': lat, 'lon': lon, 'azi': azi, 'tech': tech,
                        'info': {c: getattr(r, c) or '' for c in cols}
                    })
            except: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Bản đồ Trực quan (GIS)", active_page='gis', selected_tech=tech, site_code_input=site_code_input, cell_name_input=cell_name_input, gis_data=gis_data, its_data=[], show_its=False, action_type=action_type)

@app.route('/kpi')
@login_required
def kpi():
    selected_tech = request.args.get('tech', '4g')
    cell_name_input = request.args.get('cell_name', '').strip()
    charts = {}
    target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]
    KPI_Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)

    if target_cells and KPI_Model:
        data = KPI_Model.query.filter(KPI_Model.ten_cell.in_(target_cells)).all()
        if data:
            data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
            all_labels = sorted(list(set([x.thoi_gian for x in data])), key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
            data_by_cell = defaultdict(list)
            for x in data: data_by_cell[str(x.ten_cell).strip().upper()].append(x)
            colors = generate_colors(10)
            
            metrics = [{'key': 'traffic', 'label': 'Traffic'}, {'key': 'user_dl_avg_thput', 'label': 'Thput'}] if selected_tech=='4g' else [{'key': 'traffic', 'label': 'Traffic'}]
            
            for metric in metrics:
                datasets = []
                for i, cell_code in enumerate(target_cells):
                    cell_data = data_by_cell.get(cell_code.upper(), [])
                    d_map = {item.thoi_gian: getattr(item, metric['key'], 0) for item in cell_data}
                    datasets.append({'label': cell_code, 'data': [d_map.get(lbl, 0) for lbl in all_labels], 'borderColor': colors[i % len(colors)], 'fill': False})
                charts[f"chart_{metric['key']}"] = {'title': metric['label'], 'labels': all_labels, 'datasets': datasets}

    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi', selected_tech=selected_tech, cell_name_input=cell_name_input, charts=charts, selected_poi='', poi_list=[])

@app.route('/qoe-qos')
@login_required
def qoe_qos():
    cell_name_input = request.args.get('cell_name', '').strip()
    charts = {}
    has_data = False
    qoe_details, qos_details, qoe_headers, qos_headers = [], [], [], []
    
    if cell_name_input:
        qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoE4G.id.asc()).all()
        qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoS4G.id.asc()).all()
        
        if qoe_records or qos_records:
            has_data = True
            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))
            
            if qoe_records:
                qoe_scores = [{r.week_name: r.qoe_score or 0 for r in qoe_records}.get(w, None) for w in all_weeks]
                charts['qoe_score_chart'] = {'title': 'Biểu đồ Điểm QoE', 'labels': all_weeks, 'datasets': [{'label': 'Điểm QoE (1-5)', 'data': qoe_scores, 'borderColor': '#0078d4', 'fill': False}]}
                for r in qoe_records:
                    if r.details:
                        try:
                            d = json.loads(r.details)
                            if not qoe_headers: qoe_headers = list(d.keys())
                            qoe_details.append({'week': r.week_name, 'data': d})
                        except: pass
            
            if qos_records:
                qos_scores = [{r.week_name: r.qos_score or 0 for r in qos_records}.get(w, None) for w in all_weeks]
                charts['qos_score_chart'] = {'title': 'Biểu đồ Điểm QoS', 'labels': all_weeks, 'datasets': [{'label': 'Điểm QoS (1-5)', 'data': qos_scores, 'borderColor': '#ffaa44', 'fill': False}]}
                for r in qos_records:
                    if r.details:
                        try:
                            d = json.loads(r.details)
                            if not qos_headers: qos_headers = list(d.keys())
                            qos_details.append({'week': r.week_name, 'data': d})
                        except: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="QoE & QoS", active_page='qoe_qos', cell_name_input=cell_name_input, charts=charts, has_data=has_data, qoe_details=qoe_details, qos_details=qos_details, qoe_headers=qoe_headers, qos_headers=qos_headers)

@app.route('/poi')
@login_required
def poi(): return render_page(CONTENT_TEMPLATE, title="POI Report", active_page='poi', poi_list=[])

@app.route('/conges-3g')
@login_required
def conges_3g(): return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=[])

@app.route('/worst-cell')
@login_required
def worst_cell(): return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=[])

@app.route('/traffic-down')
@login_required
def traffic_down(): return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', zero_traffic=[], degraded=[], degraded_pois=[])

@app.route('/script')
@login_required
def script(): return render_page(CONTENT_TEMPLATE, title="Script", active_page='script')

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '4g')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    rows = Model.query.limit(100).all()
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    data = [{c: getattr(r, c) for c in cols} | {'id': r.id} for r in rows]
    return render_page(CONTENT_TEMPLATE, title="RF Data", active_page='rf', current_tech=tech, rf_columns=cols, rf_data=data)

@app.route('/rf/delete/<tech>/<int:id>')
@login_required
def rf_delete(tech, id):
    if current_user.role != 'admin': return redirect(url_for('rf', tech=tech))
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db.session.delete(db.session.get(Model, id)); db.session.commit()
    return redirect(url_for('rf', tech=tech))

@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.form.get('type')
        if itype in ['qoe4g', 'qos4g']:
            week_name = request.form.get('week_name', 'Tuần')
            TargetModel = QoE4G if itype == 'qoe4g' else QoS4G
            for file in files:
                try:
                    df = pd.read_excel(file, header=None) if file.filename.endswith('.xlsx') else pd.read_csv(file, header=None)
                    header_row_idx, cell_col_idx = -1, -1
                    for i, row in df.iterrows():
                        for j, val in enumerate(row):
                            if str(val).lower().strip() in ['cell name', 'tên cell', 'cell_name']:
                                header_row_idx, cell_col_idx = i, j
                                break
                        if header_row_idx != -1: break
                        
                    if header_row_idx != -1 and cell_col_idx != -1:
                        headers = [" - ".join([str(df.iloc[i, j]).strip() for i in range(header_row_idx + 1) if str(df.iloc[i, j]).strip() not in ['nan', 'None', '']]) or f"Col_{j}" for j in range(len(df.columns))]
                        records = []
                        for i in range(header_row_idx + 1, len(df)):
                            row_data = df.iloc[i]
                            c_name = str(row_data[cell_col_idx]).strip()
                            if not c_name or c_name == 'nan' or len(c_name) < 3 or c_name.isdigit(): continue
                            
                            try: val1 = float(row_data[cell_col_idx + 2])
                            except: val1 = 0.0
                            if math.isnan(val1): val1 = 0.0
                                
                            try: val2 = float(row_data[cell_col_idx + 3])
                            except: val2 = 0.0
                            if math.isnan(val2): val2 = 0.0
                                
                            percent, score = max(val1, val2), min(val1, val2)
                            
                            details_dict = {headers[j]: str(row_data[j]).strip() for j in range(len(headers)) if pd.notna(row_data[j]) and str(row_data[j]).strip() != 'nan'}
                            details_json = json.dumps(details_dict, ensure_ascii=False)
                            
                            records.append({'cell_name': c_name, 'week_name': week_name, 'qoe_score' if itype == 'qoe4g' else 'qos_score': score, 'qoe_percent' if itype == 'qoe4g' else 'qos_percent': percent, 'details': details_json})
                        if records:
                            db.session.bulk_insert_mappings(TargetModel, records)
                            db.session.commit()
                            flash(f'Import thành công {len(records)} dòng.', 'success')
                except Exception as e: flash(f'Lỗi: {e}', 'danger')
        return redirect(url_for('import_data'))
    return render_page(CONTENT_TEMPLATE, title="Data Import", active_page='import', kpi_rows=[])

@app.route('/backup')
@login_required
def backup_restore(): return render_page(BACKUP_RESTORE_TEMPLATE, title="Backup", active_page='backup_restore')

@app.route('/users')
@login_required
def manage_users(): return render_page(USER_MANAGEMENT_TEMPLATE, users=User.query.all(), active_page='users')

@app.route('/profile')
@login_required
def profile(): return render_page(PROFILE_TEMPLATE, active_page='profile')

if __name__ == '__main__':
    app.run(debug=True)
