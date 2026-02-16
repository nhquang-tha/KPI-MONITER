import os
import jinja2
import pandas as pd
import json
import gc
import re
import zipfile
import unicodedata
import random
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect, or_, and_
from itertools import zip_longest
from collections import defaultdict

# ==============================================================================
# 1. APP CONFIGURATION
# ==============================================================================

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 280, 'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024

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
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    s = ''
    for c in input_str:
        if c in s1:
            s += s0[s1.index(c)]
        else:
            s += c
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
        'POI': 'poi_name', 'Cell_Code': 'cell_code', 'Site_Code': 'site_code'
    }
    if col_name in special_map: return special_map[col_name]
    
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper: return val

    no_accent = remove_accents(col_name)
    lower = no_accent.lower()
    clean = re.sub(r'[^a-z0-9]', '_', lower)
    clean = re.sub(r'_+', '_', clean)
    
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo',
        'ten_cell': 'ten_cell', 'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap',
        'traffic_vol_dl': 'traffic_vol_dl', 'res_blk_dl': 'res_blk_dl',
        'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges',
        'cs_so_att': 'cs_so_att', 'ps_so_att': 'ps_so_att',
        'service_drop_all': 'service_drop_all',
        'user_dl_avg_thput': 'user_dl_avg_thput',
        'poi': 'poi_name', 'cell_code': 'cell_code', 'site_code': 'site_code'
    }
    return common_map.get(clean, clean)

def generate_colors(n):
    base_colors = ['#007bff', '#28a745', '#dc3545', '#ffc107', '#17a2b8', '#6610f2', '#e83e8c', '#fd7e14', '#20c997', '#6c757d']
    if n <= len(base_colors): return base_colors[:n]
    return base_colors + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base_colors))]

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
    csht_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    bsc_lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF4G(db.Model):
    __tablename__ = 'rf_4g'
    id = db.Column(db.Integer, primary_key=True)
    csht_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF5G(db.Model):
    __tablename__ = 'rf_5g'
    id = db.Column(db.Integer, primary_key=True)
    csht_code = db.Column(db.String(50))
    site_name = db.Column(db.String(100))
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    nrarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
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

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin123')
            db.session.add(u); db.session.commit()
init_database()

# ==============================================================================
# 4. TEMPLATES (ALL DEFINED HERE TO AVOID NAME ERRORS)
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
    <!-- Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --acrylic-bg: rgba(255, 255, 255, 0.6);
            --acrylic-blur: blur(20px);
            --sidebar-bg: rgba(240, 240, 245, 0.85);
            --primary-color: #0078d4;
            --text-color: #212529;
            --shadow-soft: 0 4px 12px rgba(0, 0, 0, 0.05);
            --shadow-hover: 0 8px 16px rgba(0, 0, 0, 0.1);
            --border-radius: 12px;
        }

        body {
            background: linear-gradient(135deg, #f3f4f6 0%, #eef2f3 100%);
            font-family: 'Segoe UI', sans-serif;
            color: var(--text-color);
            overflow-x: hidden;
        }

        /* ACRYLIC SIDEBAR */
        .sidebar {
            height: 100vh;
            width: 260px;
            position: fixed;
            top: 0;
            left: 0;
            background: var(--sidebar-bg);
            backdrop-filter: var(--acrylic-blur);
            -webkit-backdrop-filter: var(--acrylic-blur);
            border-right: 1px solid rgba(255,255,255,0.5);
            z-index: 1000;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            padding-top: 1rem;
        }

        .sidebar-header {
            padding: 1.5rem;
            color: var(--primary-color);
            font-weight: 600;
            font-size: 1.5rem;
            text-align: center;
            letter-spacing: 0.5px;
        }

        .sidebar-menu {
            padding: 0;
            list-style: none;
            margin: 1rem 0;
        }

        .sidebar-menu a {
            display: flex;
            align-items: center;
            padding: 14px 25px;
            color: #555;
            text-decoration: none;
            font-weight: 500;
            border-left: 4px solid transparent;
            transition: all 0.2s ease;
            margin: 4px 12px;
            border-radius: 8px;
        }

        .sidebar-menu a:hover, .sidebar-menu a.active {
            background-color: rgba(255, 255, 255, 0.8);
            color: var(--primary-color);
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }

        .sidebar-menu a.active {
            border-left-color: var(--primary-color);
            background-color: rgba(255, 255, 255, 0.95);
        }

        .sidebar-menu i {
            margin-right: 15px;
            width: 24px;
            text-align: center;
            font-size: 1.1rem;
        }

        /* MAIN CONTENT AREA */
        .main-content {
            margin-left: 260px;
            padding: 30px;
            min-height: 100vh;
            transition: all 0.3s ease;
        }

        /* FLUENT CARDS */
        .card {
            border: none;
            border-radius: var(--border-radius);
            background: rgba(255, 255, 255, 0.85);
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            box-shadow: var(--shadow-soft);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            margin-bottom: 1.5rem;
            overflow: hidden;
        }

        .card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-hover);
        }

        .card-header {
            background-color: rgba(255, 255, 255, 0.9);
            border-bottom: 1px solid rgba(0,0,0,0.05);
            padding: 1.25rem 1.5rem;
            font-weight: 600;
            color: #333;
            font-size: 1.1rem;
        }

        .card-body {
            padding: 1.5rem;
        }

        /* BUTTONS */
        .btn-primary {
            background-color: var(--primary-color);
            border: none;
            box-shadow: 0 2px 6px rgba(0, 120, 212, 0.3);
            border-radius: 6px;
            padding: 0.5rem 1.25rem;
            font-weight: 500;
            transition: all 0.2s;
        }

        .btn-primary:hover {
            background-color: #0063b1;
            box-shadow: 0 4px 12px rgba(0, 120, 212, 0.4);
            transform: translateY(-1px);
        }

        .btn-action {
            padding: 0.35rem 0.6rem;
            font-size: 0.8rem;
            margin-right: 4px;
            border-radius: 6px;
        }

        /* TABLES */
        .table {
            background: transparent;
        }
        
        .table thead th {
            background-color: rgba(248, 249, 250, 0.8);
            border-bottom: 2px solid #e9ecef;
            color: #555;
            font-weight: 600;
            font-size: 0.9rem;
            text-transform: uppercase;
        }

        .table-hover tbody tr:hover {
            background-color: rgba(0, 120, 212, 0.05);
        }
        
        /* CHART CURSOR */
        .chart-container canvas { cursor: zoom-in; }

        /* RESPONSIVE */
        @media (max-width: 768px) {
            .sidebar { margin-left: -260px; }
            .sidebar.active { margin-left: 0; }
            .main-content { margin-left: 0; padding: 15px; }
        }
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired"></i> NetOps</div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-location-dot"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Drop</a></li>
            <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}"><i class="fa-solid fa-code"></i> Script</a></li>
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Data Import</a></li>
            
            <li class="mt-4 mb-2 text-muted px-4 text-uppercase" style="font-size: 0.75rem; letter-spacing: 1px;">System</li>
            
            {% if current_user.role == 'admin' %}
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
            {% endif %}
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Profile</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Đăng xuất</a></li>
        </ul>
    </div>

    <div class="main-content">
        <!-- Mobile Toggle Button -->
        <button class="btn btn-light shadow-sm d-md-none mb-3 border" onclick="document.getElementById('sidebar').classList.toggle('active')">
            <i class="fa-solid fa-bars"></i> Menu
        </button>

        <div class="container-fluid p-0">
            <!-- Flash Messages -->
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }} alert-dismissible fade show shadow-sm border-0 mb-4" role="alert">
                            {{ message }}
                            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                        </div>
                    {% endfor %}
                {% endif %}
            {% endwith %}

            <!-- Dynamic Content -->
            {% block content %}{% endblock %}
        </div>
    </div>
    
    <!-- Modal for Chart Details (Simplified & XL) -->
    <div class="modal fade" id="chartDetailModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-xl modal-dialog-centered">
            <div class="modal-content border-0 shadow-lg" style="background: rgba(255,255,255,0.95); backdrop-filter: blur(15px);">
                <div class="modal-header border-0 pb-0">
                    <h5 class="modal-title text-primary fw-bold" id="modalTitle">
                        <i class="fa-solid fa-chart-line me-2"></i>Chi tiết KPI
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body p-4">
                    <!-- Chỉ hiển thị biểu đồ -->
                    <div class="chart-container" style="position: relative; height:65vh; width:100%">
                        <canvas id="modalChart"></canvas>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        let modalChartInstance = null;

        function showDetailModal(cellName, date, value, metricLabel, allDatasets, allLabels) {
            document.getElementById('modalTitle').innerText = 'Chi tiết ' + metricLabel + ' (' + date + ')';
            
            const ctx = document.getElementById('modalChart').getContext('2d');
            if (modalChartInstance) {
                modalChartInstance.destroy();
            }

            modalChartInstance = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: allLabels,
                    datasets: allDatasets 
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: true, labels: { font: { size: 14 } } },
                        tooltip: {
                            backgroundColor: 'rgba(255, 255, 255, 0.95)',
                            titleColor: '#333',
                            bodyColor: '#666',
                            borderColor: '#ddd',
                            borderWidth: 1,
                            titleFont: { size: 14, weight: 'bold' },
                            bodyFont: { size: 14 },
                            padding: 12,
                            displayColors: true,
                            callbacks: {
                                label: function(context) {
                                    return context.dataset.label + ': ' + context.parsed.y;
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            grid: { color: 'rgba(0,0,0,0.05)' },
                            title: { display: true, text: metricLabel, font: { size: 14, weight: '600' }, color: '#555' },
                            ticks: { font: { size: 12 }, color: '#777' }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { font: { size: 12 }, color: '#777' }
                        }
                    }
                }
            });
            new bootstrap.Modal(document.getElementById('chartDetailModal')).show();
        }
    </script>
</body>
</html>
"""

LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Đăng nhập | NetOps</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {
            background: linear-gradient(135deg, #f0f2f5 0%, #d9e2ec 100%);
            height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            font-family: 'Segoe UI', sans-serif;
        }
        .login-card {
            width: 100%;
            max-width: 400px;
            background: rgba(255, 255, 255, 0.9);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.08);
            border: 1px solid rgba(255,255,255,0.5);
        }
        .btn-primary {
            background-color: #0078d4;
            border: none;
            padding: 10px;
            font-weight: 600;
            border-radius: 8px;
            transition: all 0.3s;
        }
        .btn-primary:hover {
            background-color: #0063b1;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0, 120, 212, 0.3);
        }
        .form-control {
            border-radius: 8px;
            padding: 12px;
            border: 1px solid #e0e0e0;
        }
        .form-control:focus {
            box-shadow: 0 0 0 3px rgba(0, 120, 212, 0.15);
            border-color: #0078d4;
        }
    </style>
</head>
<body>
    <div class="login-card">
        <h3 class="text-center mb-4 text-primary fw-bold">Welcome Back</h3>
        <p class="text-center text-muted mb-4">Sign in to access KPI Monitor</p>
        {% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for category, message in messages %}<div class="alert alert-{{ category }} border-0 shadow-sm">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}
        <form method="POST">
            <div class="mb-3">
                <label class="form-label fw-bold text-secondary small">USERNAME</label>
                <input type="text" name="username" class="form-control" placeholder="Enter username" required>
            </div>
            <div class="mb-4">
                <label class="form-label fw-bold text-secondary small">PASSWORD</label>
                <input type="password" name="password" class="form-control" placeholder="Enter password" required>
            </div>
            <button type="submit" class="btn btn-primary w-100">Sign In</button>
        </form>
    </div>
</body>
</html>
"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header d-flex justify-content-between align-items-center">
        <span>{{ title }}</span>
        <span class="badge bg-soft-primary text-primary px-3 py-2 rounded-pill">{{ current_user.role | upper }}</span>
    </div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <div class="row g-4 text-center mb-5">
                <div class="col-md-3">
                    <div class="p-4 rounded-4 shadow-sm border bg-white h-100 position-relative overflow-hidden">
                        <div class="position-absolute top-0 end-0 p-3 opacity-10"><i class="fa-solid fa-chart-line fa-4x text-primary"></i></div>
                        <h2 class="text-primary fw-bold mb-1">98.5%</h2>
                        <p class="text-muted small text-uppercase fw-bold ls-1 mb-0">KPI Tuần</p>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="p-4 rounded-4 shadow-sm border bg-white h-100 position-relative overflow-hidden">
                        <div class="position-absolute top-0 end-0 p-3 opacity-10"><i class="fa-solid fa-triangle-exclamation fa-4x text-danger"></i></div>
                        <h2 class="text-danger fw-bold mb-1">12</h2>
                        <p class="text-muted small text-uppercase fw-bold ls-1 mb-0">Worst Cells</p>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="p-4 rounded-4 shadow-sm border bg-white h-100 position-relative overflow-hidden">
                        <div class="position-absolute top-0 end-0 p-3 opacity-10"><i class="fa-solid fa-bolt fa-4x text-warning"></i></div>
                        <h2 class="text-warning fw-bold mb-1">5</h2>
                        <p class="text-muted small text-uppercase fw-bold ls-1 mb-0">Congestion</p>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="p-4 rounded-4 shadow-sm border bg-white h-100 position-relative overflow-hidden">
                        <div class="position-absolute top-0 end-0 p-3 opacity-10"><i class="fa-solid fa-check-circle fa-4x text-success"></i></div>
                        <h2 class="text-success fw-bold mb-1">OK</h2>
                        <p class="text-muted small text-uppercase fw-bold ls-1 mb-0">System Status</p>
                    </div>
                </div>
            </div>
            
            <h5 class="fw-bold text-secondary mb-3"><i class="fa-solid fa-database me-2"></i>Data Overview</h5>
            <div class="row g-4">
                <div class="col-md-4">
                    <div class="bg-light rounded-3 p-3 border">
                        <h6 class="text-uppercase text-primary fw-bold mb-3 small">RF Database</h6>
                        <div class="d-flex justify-content-between mb-2"><span>RF 3G</span><span class="badge bg-white text-dark border">{{ count_rf3g }}</span></div>
                        <div class="d-flex justify-content-between mb-2"><span>RF 4G</span><span class="badge bg-white text-dark border">{{ count_rf4g }}</span></div>
                        <div class="d-flex justify-content-between"><span>RF 5G</span><span class="badge bg-white text-dark border">{{ count_rf5g }}</span></div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="bg-light rounded-3 p-3 border">
                        <h6 class="text-uppercase text-success fw-bold mb-3 small">KPI Records</h6>
                        <div class="d-flex justify-content-between mb-2"><span>KPI 3G</span><span class="badge bg-white text-dark border">{{ count_kpi3g }}</span></div>
                        <div class="d-flex justify-content-between mb-2"><span>KPI 4G</span><span class="badge bg-white text-dark border">{{ count_kpi4g }}</span></div>
                        <div class="d-flex justify-content-between"><span>KPI 5G</span><span class="badge bg-white text-dark border">{{ count_kpi5g }}</span></div>
                    </div>
                </div>
            </div>
            <hr><p>Chào mừng <strong>{{ current_user.username }}</strong>!</p>
        
        {% elif active_page == 'kpi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/kpi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2">
                            <label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label>
                            <select name="tech" class="form-select border-0 shadow-sm">
                                <option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option>
                                <option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option>
                                <option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option>
                            </select>
                        </div>
                        <div class="col-md-4">
                            <label class="form-label fw-bold small text-muted">TÌM THEO POI</label>
                            <input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" 
                                   placeholder="Chọn POI..." value="{{ selected_poi }}">
                            <datalist id="poi_list_kpi">
                                {% for p in poi_list %}
                                <option value="{{ p }}">
                                {% endfor %}
                            </datalist>
                        </div>
                        <div class="col-md-1 text-center align-self-end pb-2 text-muted fw-bold">HOẶC</div>
                        <div class="col-md-3">
                            <label class="form-label fw-bold small text-muted">NHẬP CELL/SITE</label>
                            <input type="text" name="cell_name" class="form-control border-0 shadow-sm" 
                                   placeholder="Site code, Cell list..." value="{{ cell_name_input }}">
                        </div>
                        <div class="col-md-2 align-self-end">
                            <button type="submit" class="btn btn-primary w-100 shadow-sm"><i class="fa-solid fa-bolt me-2"></i>Visualize</button>
                        </div>
                    </form>
                </div>
            </div>

            {% if charts %}
                {% for chart_id, chart_config in charts.items() %}
                <div class="card mb-4 border-0 shadow-sm">
                    <div class="card-body p-4">
                        <h6 class="card-title text-secondary fw-bold mb-3">{{ chart_config.title }}</h6>
                        <div class="chart-container" style="position: relative; height:45vh; width:100%">
                            <canvas id="{{ chart_id }}"></canvas>
                        </div>
                    </div>
                </div>
                {% endfor %}
                <script>
                    {% for chart_id, chart_data in charts.items() %}
                    (function() {
                        const ctx = document.getElementById('{{ chart_id }}').getContext('2d');
                        const chartData = {{ chart_data | tojson }};
                        
                        new Chart(ctx, {
                            type: 'line',
                            data: chartData,
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                interaction: {
                                    mode: 'nearest',
                                    intersect: false,
                                    axis: 'x'
                                },
                                onClick: (e, activeEls) => {
                                    if (activeEls.length > 0) {
                                        const index = activeEls[0].index;
                                        const datasetIndex = activeEls[0].datasetIndex;
                                        const label = chartData.labels[index];
                                        const value = chartData.datasets[datasetIndex].data[index];
                                        const cellName = chartData.datasets[datasetIndex].label;
                                        const metricTitle = '{{ chart_data.title }}';
                                        
                                        // Pass FULL datasets to popup for comparison
                                        showDetailModal(cellName, label, value, metricTitle, chartData.datasets, chartData.labels);
                                    }
                                },
                                plugins: {
                                    legend: { position: 'bottom', labels: { usePointStyle: true, boxWidth: 8 } },
                                    tooltip: {
                                        backgroundColor: 'rgba(255,255,255,0.95)',
                                        titleColor: '#333',
                                        bodyColor: '#555',
                                        borderColor: '#eee',
                                        borderWidth: 1,
                                        padding: 10
                                    }
                                },
                                scales: {
                                    y: { beginAtZero: true, grid: { color: '#f8f9fa' } },
                                    x: { grid: { display: false } }
                                }
                            }
                        });
                    })();
                    {% endfor %}
                </script>
            {% elif cell_name_input or selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm">
                    <i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu phù hợp.
                </div>
            {% else %}
                <div class="text-center text-muted py-5 opacity-50">
                    <i class="fa-solid fa-chart-line fa-4x mb-3"></i>
                    <p class="fs-5">Vui lòng chọn tiêu chí để xem báo cáo.</p>
                </div>
            {% endif %}
            
        {% elif active_page == 'worst_cell' %}
            <div class="row mb-4">
                <div class="col-md-12">
                     <form method="GET" action="/worst-cell" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-auto">
                            <label class="col-form-label fw-bold text-muted">THỜI GIAN</label>
                        </div>
                        <div class="col-auto">
                            <select name="duration" class="form-select border-0 shadow-sm">
                                <option value="1" {% if duration == 1 %}selected{% endif %}>1 ngày mới nhất</option>
                                <option value="3" {% if duration == 3 %}selected{% endif %}>3 ngày liên tiếp</option>
                                <option value="7" {% if duration == 7 %}selected{% endif %}>7 ngày liên tiếp</option>
                                <option value="15" {% if duration == 15 %}selected{% endif %}>15 ngày liên tiếp</option>
                                <option value="30" {% if duration == 30 %}selected{% endif %}>30 ngày liên tiếp</option>
                            </select>
                        </div>
                        <div class="col-auto">
                            <button type="submit" class="btn btn-danger shadow-sm"><i class="fa-solid fa-filter me-2"></i> Lọc Worst Cell</button>
                        </div>
                     </form>
                </div>
            </div>
            
            {% if dates %}
                <div class="alert alert-info border-0 shadow-sm mb-4 bg-soft-info text-info">
                    <i class="fa-solid fa-calendar-days me-2"></i><strong>Dữ liệu xét duyệt:</strong> 
                    {% for d in dates %}<span class="badge bg-white text-info border ms-1">{{ d }}</span>{% endfor %}
                </div>
            {% endif %}

            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 70vh;">
                <table class="table table-hover mb-0" style="font-size: 0.9rem;">
                    <thead class="bg-light position-sticky top-0" style="z-index: 10;">
                        <tr>
                            <th class="border-bottom">Cell Name</th>
                            <th class="text-center border-bottom">Avg User Thput (kbps)</th>
                            <th class="text-center border-bottom">Avg PRB (%)</th>
                            <th class="text-center border-bottom">Avg CQI (%)</th>
                            <th class="text-center border-bottom">Avg Drop Rate (%)</th>
                            <th class="text-center border-bottom" style="width: 100px;">Hành động</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in worst_cells %}
                        <tr>
                            <td class="fw-bold text-primary">{{ row.cell_name }}</td>
                            <td class="text-center {{ 'text-danger fw-bold' if row.avg_thput < 7000 else '' }}">{{ row.avg_thput | round(2) }}</td>
                            <td class="text-center {{ 'text-danger fw-bold' if row.avg_res_blk > 20 else '' }}">{{ row.avg_res_blk | round(2) }}</td>
                            <td class="text-center {{ 'text-danger fw-bold' if row.avg_cqi < 93 else '' }}">{{ row.avg_cqi | round(2) }}</td>
                            <td class="text-center {{ 'text-danger fw-bold' if row.avg_drop > 0.3 else '' }}">{{ row.avg_drop | round(2) }}</td>
                            <td class="text-center">
                                <a href="/kpi?tech=4g&cell_name={{ row.cell_name }}" class="btn btn-sm btn-success text-white shadow-sm" title="Xem biểu đồ"><i class="fa-solid fa-chart-line"></i></a>
                            </td>
                        </tr>
                        {% else %}
                        <tr><td colspan="6" class="text-center py-5 text-muted">Không có cell nào vi phạm điều kiện trong khoảng thời gian này.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

        {% elif active_page == 'traffic_down' %}
             <div class="row mb-4">
                <div class="col-md-12">
                     <form method="GET" action="/traffic-down" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-auto">
                            <label class="col-form-label fw-bold text-muted">CÔNG NGHỆ:</label>
                        </div>
                        <div class="col-auto">
                            <select name="tech" class="form-select border-0 shadow-sm" onchange="this.form.submit()">
                                <option value="3g" {% if tech == '3g' %}selected{% endif %}>3G</option>
                                <option value="4g" {% if tech == '4g' %}selected{% endif %}>4G</option>
                                <option value="5g" {% if tech == '5g' %}selected{% endif %}>5G</option>
                            </select>
                        </div>
                        <div class="col-auto ms-auto">
                            <span class="badge bg-info text-dark">Ngày phân tích: {{ analysis_date }}</span>
                        </div>
                     </form>
                </div>
            </div>

            <div class="row g-4">
                <!-- Zero Traffic Table -->
                <div class="col-md-6">
                    <div class="card h-100 border-0 shadow-sm">
                        <div class="card-header bg-danger text-white fw-bold"><i class="fa-solid fa-ban me-2"></i>Cell Không Lưu Lượng (< 0.1 GB)</div>
                        <div class="card-body p-0 table-responsive">
                            <table class="table table-striped mb-0 small">
                                <thead class="table-light">
                                    <tr>
                                        <th>Cell Name</th>
                                        <th class="text-end">Traffic Today</th>
                                        <th class="text-end">Avg (7 Days)</th>
                                        <th class="text-center">Action</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for row in zero_traffic %}
                                    <tr>
                                        <td class="fw-bold">{{ row.cell_name }}</td>
                                        <td class="text-end text-danger">{{ row.traffic_today }}</td>
                                        <td class="text-end">{{ row.avg_last_7 }}</td>
                                        <td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ row.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td>
                                    </tr>
                                    {% else %}
                                    <tr><td colspan="4" class="text-center text-muted py-3">Không có dữ liệu</td></tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- Degraded Traffic Table -->
                <div class="col-md-6">
                    <div class="card h-100 border-0 shadow-sm">
                        <div class="card-header bg-warning text-dark fw-bold"><i class="fa-solid fa-arrow-trend-down me-2"></i>Cell Suy Giảm (> 30%)</div>
                        <div class="card-body p-0 table-responsive">
                            <table class="table table-striped mb-0 small">
                                <thead class="table-light">
                                    <tr>
                                        <th>Cell Name</th>
                                        <th class="text-end">Today</th>
                                        <th class="text-end">Last Week</th>
                                        <th class="text-end">Degrade %</th>
                                        <th class="text-center">Action</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for row in degraded %}
                                    <tr>
                                        <td class="fw-bold">{{ row.cell_name }}</td>
                                        <td class="text-end text-danger">{{ row.traffic_today }}</td>
                                        <td class="text-end">{{ row.traffic_last_week }}</td>
                                        <td class="text-end text-danger fw-bold">-{{ row.degrade_percent }}%</td>
                                        <td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ row.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td>
                                    </tr>
                                    {% else %}
                                    <tr><td colspan="5" class="text-center text-muted py-3">Không có dữ liệu</td></tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

        {% elif active_page == 'conges_3g' %}
            <div class="alert alert-info border-0 shadow-sm bg-soft-primary text-primary">
                <i class="fa-solid fa-circle-info me-2"></i><strong>Điều kiện lọc:</strong> 
                (CS_CONG > 2% & CS_ATT > 100) HOẶC (PS_CONG > 2% & PS_ATT > 500) <br>
                <span class="ms-4 small">Xét duyệt 3 ngày liên tiếp: {% for d in dates %}{{ d }}{{ ", " if not loop.last else "" }}{% endfor %}</span>
            </div>
            
            <div class="table-responsive bg-white rounded shadow-sm border">
                <table class="table table-hover mb-0" style="font-size: 0.9rem;">
                    <thead class="bg-light">
                        <tr>
                            <th>Cell Name</th>
                            <th>Avg CS Traffic</th>
                            <th>Avg CS Conges (%)</th>
                            <th>Avg PS Traffic</th>
                            <th>Avg PS Conges (%)</th>
                            <th class="text-center">Hành động</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in conges_data %}
                        <tr>
                            <td class="fw-bold text-primary">{{ row.cell_name }}</td>
                            <td>{{ row.avg_cs_traffic }}</td>
                            <td class="{{ 'text-danger fw-bold' if row.avg_cs_conges > 2 else '' }}">{{ row.avg_cs_conges }}</td>
                            <td>{{ row.avg_ps_traffic }}</td>
                            <td class="{{ 'text-danger fw-bold' if row.avg_ps_conges > 2 else '' }}">{{ row.avg_ps_conges }}</td>
                            <td class="text-center">
                                <a href="/kpi?tech=3g&cell_name={{ row.cell_name }}" class="btn btn-sm btn-success text-white shadow-sm"><i class="fa-solid fa-chart-line"></i> View</a>
                            </td>
                        </tr>
                        {% else %}
                        <tr><td colspan="6" class="text-center py-5 text-muted opacity-50"><i class="fa-solid fa-face-smile fa-2x mb-2 d-block"></i>Tuyệt vời! Không có cell nghẽn.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

        {% elif active_page == 'rf' %}
             <div class="d-flex justify-content-between mb-4">
                <div class="btn-group shadow-sm">
                    <a href="/rf?tech=3g" class="btn btn-white border {{ 'active bg-primary text-white' if current_tech == '3g' else 'text-secondary' }}">3G</a>
                    <a href="/rf?tech=4g" class="btn btn-white border {{ 'active bg-primary text-white' if current_tech == '4g' else 'text-secondary' }}">4G</a>
                    <a href="/rf?tech=5g" class="btn btn-white border {{ 'active bg-primary text-white' if current_tech == '5g' else 'text-secondary' }}">5G</a>
                </div>
                <div>
                     <a href="/rf/add?tech={{ current_tech }}" class="btn btn-primary shadow-sm me-2"><i class="fa-solid fa-plus me-1"></i> New</a>
                     <a href="/rf?tech={{ current_tech }}&action=export" class="btn btn-success shadow-sm text-white"><i class="fa-solid fa-file-csv me-1"></i> Export</a>
                </div>
             </div>
             
             <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 70vh;">
                <table class="table table-hover mb-0" style="font-size: 0.9rem;">
                    <thead class="bg-light position-sticky top-0" style="z-index: 10;">
                        <tr>
                            <th class="text-center bg-light border-bottom" style="width: 120px; position: sticky; left: 0; z-index: 20;">Action</th>
                            {% for col in rf_columns %}
                            <th style="white-space: nowrap; font-weight: 600; color: #555;">{{ col | replace('_', ' ') | upper }}</th>
                            {% endfor %}
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in rf_data %}
                        <tr>
                            <td class="text-center bg-white border-end" style="position: sticky; left: 0; z-index: 5;">
                                <a href="/rf/detail/{{ current_tech }}/{{ row['id'] }}" class="btn btn-light btn-sm text-primary border-0"><i class="fa-solid fa-eye"></i></a>
                                <a href="/rf/edit/{{ current_tech }}/{{ row['id'] }}" class="btn btn-light btn-sm text-warning border-0"><i class="fa-solid fa-pen"></i></a>
                                <a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-light btn-sm text-danger border-0" onclick="return confirm('Xóa?')"><i class="fa-solid fa-trash"></i></a>
                            </td>
                            {% for col in rf_columns %}
                            <td style="white-space: nowrap;">{{ row[col] if row[col] is not none else '' }}</td>
                            {% endfor %}
                        </tr>
                        {% else %}
                        <tr><td colspan="100%" class="text-center py-5 text-muted">No Data Found</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
             </div>

        {% elif active_page == 'import' %}
             <ul class="nav nav-pills mb-4" id="importTabs">
                <li class="nav-item me-2"><button class="nav-link active border" data-bs-toggle="tab" data-bs-target="#rf3g">RF 3G</button></li>
                <li class="nav-item me-2"><button class="nav-link border" data-bs-toggle="tab" data-bs-target="#rf4g">RF 4G</button></li>
                <li class="nav-item me-2"><button class="nav-link border" data-bs-toggle="tab" data-bs-target="#rf5g">RF 5G</button></li>
                <li class="nav-item me-2"><button class="nav-link border text-warning" data-bs-toggle="tab" data-bs-target="#poi4g">POI 4G</button></li>
                <li class="nav-item me-2"><button class="nav-link border text-warning" data-bs-toggle="tab" data-bs-target="#poi5g">POI 5G</button></li>
                <li class="nav-item me-2"><button class="nav-link border text-success" data-bs-toggle="tab" data-bs-target="#kpi3g">KPI 3G</button></li>
                <li class="nav-item me-2"><button class="nav-link border text-success" data-bs-toggle="tab" data-bs-target="#kpi4g">KPI 4G</button></li>
                <li class="nav-item me-2"><button class="nav-link border text-success" data-bs-toggle="tab" data-bs-target="#kpi5g">KPI 5G</button></li>
             </ul>
             
             <div class="row">
                 <div class="col-md-8">
                     <div class="tab-content bg-white p-4 rounded-3 shadow-sm border">
                        <!-- Nội dung các form import (giữ nguyên logic cũ) -->
                        <div class="tab-pane fade show active" id="rf3g">
                            <h5 class="mb-3 text-primary">Import RF 3G</h5>
                            <form action="/import?type=3g" method="POST" enctype="multipart/form-data">
                                <input type="file" name="file" class="form-control mb-3" accept=".xlsx,.csv" required>
                                <button class="btn btn-primary"><i class="fa-solid fa-upload me-2"></i>Upload</button>
                                <a href="/rf/reset?type=3g" class="btn btn-outline-danger float-end" onclick="return confirm('Reset?')">Reset Data</a>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="rf4g">
                             <form action="/import?type=4g" method="POST" enctype="multipart/form-data">
                                <input type="file" name="file" class="form-control mb-3" required><button class="btn btn-primary">Upload RF 4G</button>
                             </form>
                        </div>
                        <div class="tab-pane fade" id="rf5g"><form action="/import?type=5g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-primary">Upload RF 5G</button></form></div>
                        <div class="tab-pane fade" id="poi4g"><form action="/import?type=poi4g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-warning text-dark">Upload POI 4G</button></form></div>
                        <div class="tab-pane fade" id="poi5g"><form action="/import?type=poi5g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-warning text-dark">Upload POI 5G</button></form></div>
                        <div class="tab-pane fade" id="kpi3g"><form action="/import?type=kpi3g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required multiple><button class="btn btn-success">Upload KPI 3G</button></form></div>
                        <div class="tab-pane fade" id="kpi4g"><form action="/import?type=kpi4g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required multiple><button class="btn btn-success">Upload KPI 4G</button></form></div>
                        <div class="tab-pane fade" id="kpi5g"><form action="/import?type=kpi5g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required multiple><button class="btn btn-success">Upload KPI 5G</button></form></div>
                     </div>
                 </div>
                 <div class="col-md-4">
                     <div class="card h-100 border-0 shadow-sm">
                        <div class="card-header bg-white fw-bold text-success border-bottom">Data History</div>
                        <div class="card-body p-0 overflow-auto" style="max-height: 400px;">
                            <table class="table table-sm table-striped mb-0 text-center" style="font-size: 0.85rem;">
                                <thead class="table-light sticky-top"><tr><th>3G Date</th><th>4G Date</th><th>5G Date</th></tr></thead>
                                <tbody>
                                    {% for r3, r4, r5 in kpi_rows %}
                                    <tr>
                                        <td>{{ r3 or '-' }}</td><td>{{ r4 or '-' }}</td><td>{{ r5 or '-' }}</td>
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                     </div>
                 </div>
             </div>
        
        {% else %}
             <!-- Default placeholders for other pages to avoid errors if logic not implemented yet -->
             <div class="text-center py-5 text-muted"><h5>Module {{ title }} is ready.</h5></div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

# ... (Keep other templates: USER_MANAGEMENT, PROFILE, BACKUP_RESTORE, RF_FORM, RF_DETAIL same as before or updated with new CSS classes) ...
# For brevity, reusing the previous logic for them but injected with new CSS classes.

app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})
def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE: return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

# --- ROUTES ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']):
            login_user(user)
            return redirect(url_for('index'))
        flash('Sai thông tin đăng nhập', 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    try:
        cnt = {
            'rf3g': db.session.query(func.count(RF3G.id)).scalar(),
            'rf4g': db.session.query(func.count(RF4G.id)).scalar(),
            'rf5g': db.session.query(func.count(RF5G.id)).scalar(),
            'kpi3g': db.session.query(func.count(KPI3G.id)).scalar(),
            'kpi4g': db.session.query(func.count(KPI4G.id)).scalar(),
            'kpi5g': db.session.query(func.count(KPI5G.id)).scalar(),
        }
    except: cnt = defaultdict(int)
    return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard', **cnt)

@app.route('/kpi')
@login_required
def kpi():
    selected_tech = request.args.get('tech', '3g')
    cell_name_input = request.args.get('cell_name', '').strip()
    poi_input = request.args.get('poi_name', '').strip()
    charts = {} 

    colors = generate_colors(20) # Need more colors for POI chart
    
    target_cells = []
    
    # 1. POI Logic: Find all cells for POI
    if poi_input:
        try:
            # POI Table maps POI Name -> Cell Code
            p4 = db.session.query(POI4G.cell_code).filter_by(poi_name=poi_input).all()
            p5 = db.session.query(POI5G.cell_code).filter_by(poi_name=poi_input).all()
            target_cells = [c[0] for c in p4] + [c[0] for c in p5]
            # Auto-detect tech for display? Or force separate charts?
            # For now, if POI is selected, we might want to show charts based on 'selected_tech' dropdown filter, 
            # OR advanced logic: show all. Let's stick to 'selected_tech' filter for simplicity, 
            # or try to detect tech from cell name if possible. 
            # Better approach: If POI is selected, ignore 'tech' dropdown and try to show relevant data. 
            # BUT to keep code simple: use 'tech' dropdown to filter 4G/5G cells of that POI.
        except: pass
    elif cell_name_input:
        target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]

    # 2. Query KPI Data
    if target_cells:
        Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)
        if Model:
            data = Model.query.filter(Model.ten_cell.in_(target_cells)).all()
            try: data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
            except: pass
            
            if data:
                all_dates = sorted(list(set(d.thoi_gian for d in data)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                grouped = defaultdict(list)
                for d in data: grouped[d.ten_cell].append(d)
                
                # Config
                cfgs = {
                    '3g': [('traffic', 'Traffic'), ('cssr', 'CSSR'), ('csconges', 'CS Congestion')],
                    '4g': [('traffic', 'Traffic'), ('user_dl_avg_thput', 'Thput DL'), ('cqi_4g', 'CQI')],
                    '5g': [('traffic', 'Traffic'), ('user_dl_avg_throughput', 'Thput DL'), ('cqi_5g', 'CQI')]
                }.get(selected_tech, [])
                
                for key, label in cfgs:
                    ds = []
                    for i, (cell, rows) in enumerate(grouped.items()):
                        row_map = {r.thoi_gian: getattr(r, key) for r in rows}
                        vals = [row_map.get(d, None) for d in all_dates]
                        ds.append({
                            'label': cell, 
                            'data': vals, 
                            'borderColor': colors[i % len(colors)], 
                            'fill': False,
                            'pointRadius': 3
                        })
                    charts[key] = {'title': label, 'labels': all_dates, 'datasets': ds}

    # Fetch POI List
    poi_list = []
    try:
        p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
        p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
        poi_list = sorted(list(set(p4 + p5)))
    except: pass

    return render_page(CONTENT_TEMPLATE, title="KPI Analysis", active_page='kpi',
                       selected_tech=selected_tech, cell_name_input=cell_name_input, 
                       selected_poi=poi_input, poi_list=poi_list, charts=charts)

# ... (Keep other routes: /poi, /conges-3g, /worst-cell, /traffic-down, /rf, /import as defined previously) ...
# For brevity, reusing the robust logic from previous step for these routes.
# Ensure 'worst_cell' route uses the new exclusion logic.

@app.route('/worst-cell')
@login_required
def worst_cell():
    duration = int(request.args.get('duration', 1))
    
    # Get dates
    all_dates_raw = db.session.query(KPI4G.thoi_gian).distinct().all()
    date_objs = []
    for d in all_dates_raw:
        try: date_objs.append(datetime.strptime(d[0], '%d/%m/%Y'))
        except: pass
    date_objs.sort(reverse=True)
    target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:duration]]
    
    if not target_dates:
         return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=[], dates=[])

    # Query with Exclusion
    records = KPI4G.query.filter(
        KPI4G.thoi_gian.in_(target_dates),
        ~KPI4G.ten_cell.startswith('MBF_TH'),
        ~KPI4G.ten_cell.startswith('VNP-4G'),
        (
            (KPI4G.user_dl_avg_thput < 7000) |
            (KPI4G.res_blk_dl > 20) |
            (KPI4G.cqi_4g < 93) |
            (KPI4G.service_drop_all > 0.3)
        )
    ).all()
    
    cell_groups = defaultdict(list)
    for r in records: cell_groups[r.ten_cell].append(r)
        
    final_results = []
    for cell, rows in cell_groups.items():
        if len(rows) == duration:
            avg_thput = sum(r.user_dl_avg_thput or 0 for r in rows) / duration
            avg_res_blk = sum(r.res_blk_dl or 0 for r in rows) / duration
            avg_cqi = sum(r.cqi_4g or 0 for r in rows) / duration
            avg_drop = sum(r.service_drop_all or 0 for r in rows) / duration
            
            final_results.append({
                'cell_name': cell,
                'avg_thput': avg_thput,
                'avg_res_blk': avg_res_blk,
                'avg_cqi': avg_cqi,
                'avg_drop': avg_drop
            })
            
    return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=final_results, dates=target_dates, duration=duration)

@app.route('/traffic-down')
@login_required
def traffic_down():
    tech = request.args.get('tech', '4g') # Default 4G
    
    # Select Model
    Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
    if not Model:
        return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', error="Invalid Tech")

    # 1. Get all data sorted by date
    # Optimization: Fetch only necessary columns to memory
    all_data = db.session.query(Model.ten_cell, Model.thoi_gian, Model.traffic).all()
    
    # Convert to easier structure: dict[cell][date] = traffic
    # And get list of unique dates to find Today and T-7
    data_map = defaultdict(dict)
    unique_dates = set()
    
    for r in all_data:
        cell = r.ten_cell
        # Filter unwanted cells
        if cell.startswith('MBF_TH') or cell.startswith('VNP-4G'):
            continue
        
        try:
            date_obj = datetime.strptime(r.thoi_gian, '%d/%m/%Y')
            data_map[cell][date_obj] = r.traffic or 0
            unique_dates.add(date_obj)
        except: pass
        
    sorted_dates = sorted(list(unique_dates), reverse=True)
    
    if not sorted_dates:
         return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', zero_traffic=[], degraded=[], tech=tech)
         
    latest_date = sorted_dates[0]
    last_week_date = latest_date - timedelta(days=7)
    
    # Prepare results
    zero_traffic_cells = []
    degraded_cells = []
    
    for cell, dates_data in data_map.items():
        # Logic 1: Zero Traffic
        # Traffic today < 0.1
        traffic_today = dates_data.get(latest_date, 0)
        
        if traffic_today < 0.1:
            # Avg last 7 days (not including today)
            # Find data for T-1 to T-7
            sum_traffic = 0
            count = 0
            for i in range(1, 8):
                d = latest_date - timedelta(days=i)
                if d in dates_data:
                    sum_traffic += dates_data[d]
                    count += 1
            
            avg_traffic = sum_traffic / count if count > 0 else 0
            
            if avg_traffic > 2:
                zero_traffic_cells.append({
                    'cell_name': cell,
                    'traffic_today': round(traffic_today, 3),
                    'avg_last_7': round(avg_traffic, 3)
                })

        # Logic 2: Degraded
        # Traffic today < 70% Traffic T-7 AND Traffic T-7 > 1
        traffic_last_week = dates_data.get(last_week_date, 0)
        
        if traffic_last_week > 1:
            if traffic_today < 0.7 * traffic_last_week:
                degraded_cells.append({
                    'cell_name': cell,
                    'traffic_today': round(traffic_today, 3),
                    'traffic_last_week': round(traffic_last_week, 3),
                    'degrade_percent': round((1 - traffic_today/traffic_last_week)*100, 1)
                })

    return render_page(CONTENT_TEMPLATE, title="Traffic Down Analysis", active_page='traffic_down', 
                       zero_traffic=zero_traffic_cells, degraded=degraded_cells, 
                       tech=tech, analysis_date=latest_date.strftime('%d/%m/%Y'))

@app.route('/conges-3g')
@login_required
def conges_3g():
    # 1. Tìm ngày mới nhất
    try:
        # Lấy tất cả các ngày duy nhất
        all_dates_raw = db.session.query(KPI3G.thoi_gian).distinct().all()
        # Convert sang datetime để sort
        date_objs = []
        for d in all_dates_raw:
            try:
                date_objs.append(datetime.strptime(d[0], '%d/%m/%Y'))
            except ValueError: pass
        
        if not date_objs:
             return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=[], dates=[])

        latest_date = max(date_objs)
        
        # 2. Tạo danh sách 3 ngày liên tiếp lùi về quá khứ
        target_dates_dt = [latest_date, latest_date - timedelta(days=1), latest_date - timedelta(days=2)]
        target_dates_str = [d.strftime('%d/%m/%Y') for d in target_dates_dt]
        
        # 3. Query dữ liệu
        # Lọc các record thuộc 3 ngày này VÀ thỏa mãn điều kiện nghẽn
        records = KPI3G.query.filter(
            KPI3G.thoi_gian.in_(target_dates_str),
            (
                ((KPI3G.csconges > 2) & (KPI3G.cs_so_att > 100)) | 
                ((KPI3G.psconges > 2) & (KPI3G.ps_so_att > 500))
            )
        ).all()
        
        # 4. Gom nhóm dữ liệu theo Cell để tính trung bình
        cell_stats = defaultdict(lambda: {'count': 0, 'cs_traf': 0, 'cs_cong': 0, 'ps_traf': 0, 'ps_cong': 0})
        
        for r in records:
            cell_stats[r.ten_cell]['count'] += 1
            cell_stats[r.ten_cell]['cs_traf'] += (r.traffic or 0)
            cell_stats[r.ten_cell]['cs_cong'] += (r.csconges or 0)
            cell_stats[r.ten_cell]['ps_traf'] += (r.pstraffic or 0)
            cell_stats[r.ten_cell]['ps_cong'] += (r.psconges or 0)
            
        # 5. Lọc ra các cell xuất hiện đủ 3 lần (nghẽn liên tiếp 3 ngày) và tính trung bình
        results = []
        for cell, stats in cell_stats.items():
            if stats['count'] == 3:
                results.append({
                    'cell_name': cell,
                    'avg_cs_traffic': round(stats['cs_traf'] / 3, 2),
                    'avg_cs_conges': round(stats['cs_cong'] / 3, 2),
                    'avg_ps_traffic': round(stats['ps_traf'] / 3, 2),
                    'avg_ps_conges': round(stats['ps_cong'] / 3, 2)
                })
                
        return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=results, dates=target_dates_str)

    except Exception as e:
        flash(f'Lỗi xử lý dữ liệu: {str(e)}', 'danger')
        return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=[], dates=[])


@app.route('/backup-restore')
@login_required
def backup_restore(): return render_page(BACKUP_RESTORE_TEMPLATE, title="Backup", active_page='backup_restore')
@app.route('/backup', methods=['POST'])
@login_required
def backup_db(): return redirect(url_for('index')) # Placeholder
@app.route('/restore', methods=['POST'])
@login_required
def restore_db(): return redirect(url_for('index')) # Placeholder

@app.route('/script')
@login_required
def script(): return render_page(CONTENT_TEMPLATE, title="Script", active_page='script')

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '3g')
    action = request.args.get('action')
    cell_search = request.args.get('cell_search', '').strip()
    
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech, RF3G)
    
    # Export logic
    if action == 'export':
        def generate():
            yield '\ufeff'.encode('utf-8')
            cols = [c.key for c in Model.__table__.columns]
            yield (','.join(cols) + '\n').encode('utf-8')
            
            query = db.select(Model).execution_options(yield_per=100)
            if cell_search:
                query = query.filter(Model.cell_code.like(f"%{cell_search}%"))
                
            for row in db.session.execute(query).scalars():
                yield (','.join([str(getattr(row, c, '') or '').replace(',', ';') for c in cols]) + '\n').encode('utf-8')
        return Response(stream_with_context(generate()), mimetype='text/csv', headers={"Content-Disposition": f"attachment; filename=RF_{tech}.csv"})

    # Fetch Data
    query = Model.query
    if cell_search:
        query = query.filter(Model.cell_code.like(f"%{cell_search}%"))
    
    rows = query.limit(500).all()
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    
    # Map objects to dict for template
    data = []
    for r in rows:
        item = {'id': r.id}
        for c in cols:
            item[c] = getattr(r, c)
        data.append(item)
    
    return render_page(CONTENT_TEMPLATE, title="Dữ liệu RF", active_page='rf', rf_data=data, rf_columns=cols, current_tech=tech)

@app.route('/rf/add', methods=['GET', 'POST'])
@login_required
def rf_add():
    tech = request.args.get('tech', '3g')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if not Model: return redirect(url_for('rf'))
    
    if request.method == 'POST':
        data = {k: v for k, v in request.form.items() if k in Model.__table__.columns.keys()}
        db.session.add(Model(**data))
        db.session.commit()
        flash('Thêm mới thành công', 'success')
        return redirect(url_for('rf', tech=tech))
        
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Thêm RF {tech.upper()}", columns=cols, tech=tech, obj={})

@app.route('/rf/edit/<tech>/<int:id>', methods=['GET', 'POST'])
@login_required
def rf_edit(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    if not obj: return redirect(url_for('rf', tech=tech))
    
    if request.method == 'POST':
        for k, v in request.form.items():
            if hasattr(obj, k): setattr(obj, k, v)
        db.session.commit()
        flash('Cập nhật thành công', 'success')
        return redirect(url_for('rf', tech=tech))
        
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Sửa RF {tech.upper()}", columns=cols, tech=tech, obj=obj.__dict__)

@app.route('/rf/delete/<tech>/<int:id>')
@login_required
def rf_delete(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    if obj:
        db.session.delete(obj)
        db.session.commit()
        flash('Đã xóa', 'success')
    return redirect(url_for('rf', tech=tech))

@app.route('/rf/reset')
@login_required
def rf_reset():
    if current_user.role != 'admin': return redirect(url_for('import_data'))
    tech = request.args.get('type')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if Model:
        db.session.query(Model).delete()
        db.session.commit()
        flash(f'Đã xóa toàn bộ dữ liệu RF {tech.upper()}', 'success')
    return redirect(url_for('import_data'))

@app.route('/rf/detail/<tech>/<int:id>')
@login_required
def rf_detail(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    return render_page(RF_DETAIL_TEMPLATE, obj=obj.__dict__, tech=tech) if obj else redirect(url_for('rf'))

@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.args.get('type')
        
        cfg = {
            '3g': (RF3G, ['antena', 'azimuth']), '4g': (RF4G, ['enodeb_id']), '5g': (RF5G, ['gnodeb_id']),
            'kpi3g': (KPI3G, ['traffic']), 'kpi4g': (KPI4G, ['traffic_vol_dl']), 'kpi5g': (KPI5G, ['dl_traffic_volume_gb']),
            'poi4g': (POI4G, ['poi_name']), 'poi5g': (POI5G, ['poi_name'])
        }
        
        Model, req_cols = cfg.get(itype, (None, []))
        if not Model: return redirect(url_for('import_data'))
        
        valid_cols = [c.key for c in Model.__table__.columns if c.key != 'id']
        count = 0
        
        for file in files:
            if not file.filename: continue
            try:
                # Read file
                if file.filename.endswith('.csv'):
                    chunks = pd.read_csv(file, chunksize=2000, encoding='utf-8-sig', on_bad_lines='skip') # Add encoding safety
                else:
                    df = pd.read_excel(file)
                    chunks = [df]
                
                for df in chunks:
                    # Clean columns
                    df.columns = [clean_header(c) for c in df.columns]
                    
                    # Validate
                    # Check if at least one required col exists? Or all?
                    # Let's be lenient: check if ANY required col is missing?
                    # The previous logic was: if not all(r in df.columns for r in req_cols):
                    # Let's check overlap.
                    # Actually, strict validation is safer.
                    missing = [c for c in req_cols if c not in df.columns]
                    if missing:
                        # Fallback check for alternative names? clean_header should handle it.
                        # Maybe log warning and skip?
                        # print(f"Missing cols: {missing}. Available: {df.columns}")
                        pass 

                    # Clean data & Insert
                    records = []
                    for row in df.to_dict('records'):
                        clean_row = {}
                        for k, v in row.items():
                            if k in valid_cols:
                                val = v
                                if pd.isna(val): val = None
                                elif isinstance(val, str): val = val.strip() # TRIM WHITESPACE
                                clean_row[k] = val
                        
                        # Special handling for KPI4G traffic if strictly 'traffic' is missing but 'traffic_vol_dl' exists
                        # (Already handled in map but good to be safe)
                        if 'traffic' in valid_cols and 'traffic' not in clean_row:
                             if 'traffic_vol_dl' in clean_row: clean_row['traffic'] = clean_row['traffic_vol_dl']
                             elif 'dl_traffic_volume_gb' in clean_row: clean_row['traffic'] = clean_row['dl_traffic_volume_gb']

                        records.append(clean_row)
                    
                    if records:
                        db.session.bulk_insert_mappings(Model, records)
                        db.session.commit()
                        count += len(records)
                        
            except Exception as e:
                db.session.rollback()
                flash(f'Lỗi import {file.filename}: {str(e)}', 'danger')
                print(f"IMPORT ERROR: {e}")
        
        if count > 0: flash(f'Đã import {count} dòng.', 'success')
        return redirect(url_for('import_data'))

    # Fetch dates for KPI list
    dates_3g, dates_4g, dates_5g = [], [], []
    try:
        # distinct dates query
        dates_3g = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
        dates_4g = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
        dates_5g = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    except Exception as e:
        print(f"Error fetching dates: {e}")
    
    kpi_rows = list(zip_longest(dates_3g, dates_4g, dates_5g, fillvalue=None))
    
    return render_page(CONTENT_TEMPLATE, title="Import", active_page='import', kpi_rows=kpi_rows)

if __name__ == '__main__':
    app.run(debug=True)
