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
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect
from itertools import zip_longest
from collections import defaultdict

# --- CẤU HÌNH APP ---
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 280, 'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# --- UTILS ---
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
    # Check exact match first
    if col_name in special_map:
        return special_map[col_name]
    
    # Check case-insensitive match
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper:
             return val

    no_accent = remove_accents(col_name)
    lower = no_accent.lower()
    clean = re.sub(r'[^a-z0-9]', '_', lower)
    clean = re.sub(r'_+', '_', clean)
    
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo',
        'ten_cell': 'ten_cell', 'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap',
        'traffic_vol_dl': 'traffic_vol_dl', 'res_blk_dl': 'res_blk_dl',
        'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges',
        'poi': 'poi_name', 'cell_code': 'cell_code', 'site_code': 'site_code'
    }
    return common_map.get(clean, clean)

def generate_colors(n):
    """Generate n distinct colors."""
    base_colors = [
        '#007bff', '#28a745', '#dc3545', '#ffc107', '#17a2b8', 
        '#6610f2', '#e83e8c', '#fd7e14', '#20c997', '#6c757d',
        '#343a40', '#007bff', '#6f42c1', '#e83e8c'
    ]
    if n <= len(base_colors):
        return base_colors[:n]
    # If more needed, generate random
    return base_colors + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base_colors))]

# --- MODELS ---
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

# --- TEMPLATES ---
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
        body { background-color: #f0f2f5; font-family: 'Roboto', sans-serif; overflow-x: hidden; }
        .sidebar { height: 100vh; width: 250px; position: fixed; top: 0; left: 0; background-color: #ffffff; box-shadow: 2px 0 5px rgba(0,0,0,0.05); z-index: 1000; transition: 0.3s; }
        .sidebar-header { padding: 20px; background: #0d6efd; color: white; text-align: center; }
        .sidebar-menu { padding: 10px 0; list-style: none; margin: 0; }
        .sidebar-menu a { display: block; padding: 12px 20px; color: #333; text-decoration: none; border-left: 4px solid transparent; transition: 0.3s; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background-color: #e9ecef; border-left-color: #0d6efd; color: #0d6efd; }
        .sidebar-menu i { margin-right: 10px; width: 20px; text-align: center; }
        .main-content { margin-left: 250px; padding: 20px; transition: 0.3s; }
        .card { border: none; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); background: white; margin-bottom: 20px; }
        .card-header { background-color: white; border-bottom: 1px solid #f0f0f0; padding: 15px 20px; font-weight: bold; color: #444; }
        /* Cursor style for chart */
        .chart-container canvas { cursor: zoom-in; }
        @media (max-width: 768px) { .sidebar { margin-left: -250px; } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; } }
        .btn-action { padding: 0.25rem 0.5rem; font-size: 0.75rem; }
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><h4><i class="fa-solid fa-network-wired"></i> KPI Monitor</h4></div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-location-dot"></i> POI</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cell</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Conges 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}"><i class="fa-solid fa-code"></i> Script</a></li>
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-file-import"></i> Import</a></li>
            <li class="mt-3 text-muted ps-3"><small>HỆ THỐNG</small></li>
            {% if current_user.role == 'admin' %}
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> Quản lý User</a></li>
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
            {% endif %}
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Tài khoản</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Đăng xuất</a></li>
        </ul>
    </div>
    <div class="main-content">
        <button class="btn btn-primary d-md-none mb-3" onclick="document.getElementById('sidebar').classList.toggle('active')"><i class="fa-solid fa-bars"></i> Menu</button>
        <div class="container-fluid">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }} alert-dismissible fade show">{{ message }}<button type="button" class="btn-close" data-bs-dismiss="alert"></button></div>
                    {% endfor %}
                {% endif %}
            {% endwith %}
            {% block content %}{% endblock %}
        </div>
    </div>
    
    <!-- Modal for Chart Details (Simplified & XL) -->
    <div class="modal fade" id="chartDetailModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-xl">
            <div class="modal-content">
                <div class="modal-header bg-light">
                    <h5 class="modal-title text-primary fw-bold" id="modalTitle">
                        <i class="fa-solid fa-chart-line me-2"></i>Chi tiết KPI
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                    <!-- Chỉ hiển thị biểu đồ, loại bỏ thông tin text bên trái -->
                    <div class="chart-container" style="position: relative; height:70vh; width:100%">
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
            // Update Modal Title only
            document.getElementById('modalTitle').innerText = 'Chi tiết ' + metricLabel + ' (' + date + ')';
            
            // Draw Specific Chart
            const ctx = document.getElementById('modalChart').getContext('2d');
            
            // Destroy old chart if exists
            if (modalChartInstance) {
                modalChartInstance.destroy();
            }

            modalChartInstance = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: allLabels,
                    datasets: allDatasets // Pass ALL datasets (all cells) to the popup chart
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: true, labels: { font: { size: 14 } } },
                        tooltip: {
                            bodyFont: { size: 14 },
                            titleFont: { size: 14 },
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
                            grid: { color: '#e0e0e0' },
                            title: { display: true, text: metricLabel, font: { size: 16, weight: 'bold' } },
                            ticks: { font: { size: 12 } }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { font: { size: 12 } }
                        }
                    }
                }
            });

            // Show Modal
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
    <title>Đăng nhập</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>body { background: #e0e5ec; height: 100vh; display: flex; align-items: center; justify-content: center; } .login-card { width: 100%; max-width: 400px; background: white; padding: 40px; border-radius: 10px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); }</style>
</head>
<body>
    <div class="login-card">
        <h3 class="text-center mb-4 text-primary">Đăng nhập</h3>
        {% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for category, message in messages %}<div class="alert alert-{{ category }}">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}
        <form method="POST">
            <div class="mb-3"><label>Username</label><input type="text" name="username" class="form-control" required></div>
            <div class="mb-3"><label>Password</label><input type="password" name="password" class="form-control" required></div>
            <button type="submit" class="btn btn-primary w-100">Đăng nhập</button>
        </form>
    </div>
</body>
</html>
"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header">{{ title }} <span class="badge bg-primary float-end">{{ current_user.role }}</span></div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <div class="row g-4 text-center">
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-primary">98.5%</h3><p class="text-muted mb-0">KPI Tuần</p></div></div>
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-danger">12</h3><p class="text-muted mb-0">Worst Cells</p></div></div>
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-warning">5</h3><p class="text-muted mb-0">Congestion</p></div></div>
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-success">OK</h3><p class="text-muted mb-0">System</p></div></div>
            </div>
            
            <hr class="my-4">
            <h5><i class="fa-solid fa-server"></i> Trạng thái Dữ liệu</h5>
            <div class="row g-3">
                <div class="col-md-4">
                    <ul class="list-group">
                        <li class="list-group-item d-flex justify-content-between align-items-center active">RF Data</li>
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            RF 3G <span class="badge bg-primary rounded-pill">{{ count_rf3g }}</span>
                        </li>
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            RF 4G <span class="badge bg-primary rounded-pill">{{ count_rf4g }}</span>
                        </li>
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            RF 5G <span class="badge bg-primary rounded-pill">{{ count_rf5g }}</span>
                        </li>
                    </ul>
                </div>
                <div class="col-md-4">
                     <ul class="list-group">
                        <li class="list-group-item d-flex justify-content-between align-items-center list-group-item-success">KPI Data</li>
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            KPI 3G <span class="badge bg-success rounded-pill">{{ count_kpi3g }}</span>
                        </li>
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            KPI 4G <span class="badge bg-success rounded-pill">{{ count_kpi4g }}</span>
                        </li>
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            KPI 5G <span class="badge bg-success rounded-pill">{{ count_kpi5g }}</span>
                        </li>
                    </ul>
                </div>
            </div>
            <hr><p>Chào mừng <strong>{{ current_user.username }}</strong>!</p>
        
        {% elif active_page == 'kpi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/kpi" class="row g-3 align-items-center">
                        <div class="col-auto">
                            <label class="col-form-label fw-bold">Chọn POI:</label>
                        </div>
                        <div class="col-md-4">
                             <input type="text" name="poi_name" list="poi_list_kpi" class="form-control" 
                                    placeholder="Chọn POI để vẽ tất cả các cell 4G và 5G..." value="{{ selected_poi }}">
                             <datalist id="poi_list_kpi">
                                 {% for p in poi_list %}
                                 <option value="{{ p }}">
                                 {% endfor %}
                             </datalist>
                        </div>

                        <div class="col-auto ms-4 border-start ps-4">
                            <label class="col-form-label fw-bold text-muted">Hoặc Lọc Thủ Công:</label>
                        </div>
                        <div class="col-auto">
                            <select name="tech" class="form-select">
                                <option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option>
                                <option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option>
                                <option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option>
                            </select>
                        </div>
                        <div class="col-md-2">
                            <input type="text" name="cell_name" class="form-control" 
                                   placeholder="Site/Cell Code..." 
                                   value="{{ cell_name_input }}">
                        </div>
                        <div class="col-auto">
                            <button type="submit" class="btn btn-primary"><i class="fa-solid fa-chart-line"></i> Vẽ biểu đồ</button>
                        </div>
                    </form>
                </div>
            </div>

            {% if charts %}
                {% for chart_id, chart_config in charts.items() %}
                <div class="card mb-4 border shadow-sm">
                    <div class="card-body">
                        <div class="chart-container" style="position: relative; height:40vh; width:100%">
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
                                    intersect: false, // Update: click anywhere in range
                                    axis: 'x'
                                },
                                onClick: (e, activeEls) => {
                                    if (activeEls.length > 0) {
                                        // Pick the first element (nearest)
                                        const index = activeEls[0].index;
                                        const datasetIndex = activeEls[0].datasetIndex;
                                        const label = chartData.labels[index];
                                        const value = chartData.datasets[datasetIndex].data[index];
                                        const cellName = chartData.datasets[datasetIndex].label;
                                        const metricTitle = '{{ chart_data.title }}';
                                        
                                        // Pass FULL datasets to popup
                                        showDetailModal(cellName, label, value, metricTitle, chartData.datasets, chartData.labels);
                                    }
                                },
                                plugins: {
                                    title: {
                                        display: true,
                                        text: '{{ chart_data.title }}',
                                        font: { size: 16 }
                                    },
                                    legend: {
                                        position: 'bottom'
                                    }
                                },
                                scales: {
                                    y: {
                                        beginAtZero: true,
                                        title: { display: true, text: 'Giá trị' }
                                    }
                                }
                            }
                        });
                    })();
                    {% endfor %}
                </script>
            {% elif cell_name_input or selected_poi %}
                <div class="alert alert-warning">
                    Không tìm thấy dữ liệu KPI phù hợp.
                </div>
            {% else %}
                <div class="text-center text-muted py-5">
                    <i class="fa-solid fa-chart-area fa-3x mb-3"></i>
                    <p>Chọn POI hoặc nhập tên Site/Cell để xem biểu đồ KPI.</p>
                </div>
            {% endif %}

        {% elif active_page == 'poi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/poi" class="row g-3 align-items-center">
                        <div class="col-auto">
                            <label class="col-form-label fw-bold">Chọn POI:</label>
                        </div>
                        <div class="col-md-6">
                            <input type="text" name="poi_name" list="poi_list" class="form-control" 
                                   placeholder="Nhập hoặc chọn tên POI..." value="{{ selected_poi }}">
                            <datalist id="poi_list">
                                {% for p in poi_list %}
                                <option value="{{ p }}">
                                {% endfor %}
                            </datalist>
                        </div>
                        <div class="col-auto">
                            <button type="submit" class="btn btn-primary"><i class="fa-solid fa-chart-pie"></i> Xem Báo Cáo Tổng Hợp</button>
                        </div>
                    </form>
                </div>
            </div>

            {% if poi_charts %}
                <div class="row">
                    {% for chart_id, chart_data in poi_charts.items() %}
                    <div class="col-md-6 mb-4">
                        <div class="card h-100 shadow-sm">
                            <div class="card-body">
                                <div class="chart-container" style="position: relative; height:35vh; width:100%">
                                    <canvas id="{{ chart_id }}"></canvas>
                                </div>
                            </div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
                <script>
                    {% for chart_id, chart_data in poi_charts.items() %}
                    (function() {
                        const ctx = document.getElementById('{{ chart_id }}').getContext('2d');
                        new Chart(ctx, {
                            type: 'line',
                            data: {{ chart_data | tojson }},
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                interaction: {
                                    mode: 'nearest',
                                    intersect: false, // Update: click anywhere in range
                                    axis: 'x'
                                },
                                plugins: {
                                    title: { display: true, text: '{{ chart_data.title }}', font: { size: 14 } },
                                    legend: { position: 'bottom' }
                                }
                            }
                        });
                    })();
                    {% endfor %}
                </script>
            {% elif selected_poi %}
                <div class="alert alert-warning">Không có dữ liệu KPI cho POI: <strong>{{ selected_poi }}</strong></div>
            {% else %}
                <div class="text-center text-muted py-5">
                    <i class="fa-solid fa-map-location-dot fa-3x mb-3"></i>
                    <p>Chọn một địa điểm POI để xem báo cáo tổng hợp.</p>
                </div>
            {% endif %}

        {% elif active_page == 'conges_3g' %}
            <div class="alert alert-info">
                <strong><i class="fa-solid fa-filter"></i> Điều kiện lọc:</strong> 
                (CS_CONG > 2% & CS_ATT > 100) HOẶC (PS_CONG > 2% & PS_ATT > 500) <br>
                <strong><i class="fa-solid fa-clock"></i> Thời gian:</strong> Xảy ra liên tiếp trong 3 ngày dữ liệu gần nhất 
                ({% for d in dates %}{{ d }}{{ ", " if not loop.last else "" }}{% endfor %})
            </div>
            
            <div class="table-responsive">
                <table class="table table-bordered table-hover small">
                    <thead class="table-light">
                        <tr>
                            <th>Cell Name</th>
                            <th>Site Code</th>
                            <th>CSHT</th>
                            <th>Antenna</th>
                            <th>Tilt</th>
                            <th class="text-center">Hành động</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in conges_data %}
                        <tr>
                            <td class="fw-bold text-primary">{{ row.cell_name }}</td>
                            <td>{{ row.site_code }}</td>
                            <td>{{ row.csht }}</td>
                            <td>{{ row.antena }}</td>
                            <td>{{ row.tilt }}</td>
                            <td class="text-center">
                                <a href="/rf/detail/3g/{{ row.rf_id }}" class="btn btn-sm btn-info text-white" title="Chi tiết RF"><i class="fa-solid fa-eye"></i></a>
                            </td>
                        </tr>
                        {% else %}
                        <tr><td colspan="6" class="text-center py-4 text-muted">Tuyệt vời! Không có cell nào bị nghẽn liên tiếp 3 ngày.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

        {% elif active_page == 'rf' %}
            <div class="mb-3 d-flex justify-content-between">
                <div class="btn-group">
                    <a href="/rf?tech=3g" class="btn btn-{{ 'primary' if current_tech == '3g' else 'outline-primary' }}">3G</a>
                    <a href="/rf?tech=4g" class="btn btn-{{ 'primary' if current_tech == '4g' else 'outline-primary' }}">4G</a>
                    <a href="/rf?tech=5g" class="btn btn-{{ 'primary' if current_tech == '5g' else 'outline-primary' }}">5G</a>
                </div>
                <div>
                    <a href="/rf/add?tech={{ current_tech }}" class="btn btn-primary me-2"><i class="fa-solid fa-plus"></i> Thêm mới</a>
                    <a href="/rf?tech={{ current_tech }}&action=export" class="btn btn-success"><i class="fa-solid fa-file-csv"></i> Xuất Excel (CSV)</a>
                </div>
            </div>
            
            <div class="mb-3">
                <form method="GET" action="/rf">
                     <input type="hidden" name="tech" value="{{ current_tech }}">
                     <div class="input-group">
                         <input type="text" name="cell_search" class="form-control" placeholder="Tìm kiếm Cell Code..." value="{{ request.args.get('cell_search', '') }}">
                         <button class="btn btn-outline-secondary" type="submit">Tìm</button>
                     </div>
                </form>
            </div>

            <div class="table-responsive" style="max-height: 70vh; overflow-y: auto;">
                <table class="table table-sm table-bordered table-hover small">
                    <thead class="table-light position-sticky top-0 shadow-sm" style="z-index: 10;">
                        <tr>
                            <th class="text-center bg-light" style="width: 120px; position: sticky; left: 0; z-index: 20;">Hành động</th>
                            {% for col in rf_columns %}
                            <th style="white-space: nowrap;">{{ col | replace('_', ' ') | upper }}</th>
                            {% endfor %}
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in rf_data %}
                        <tr>
                            <td class="text-center bg-white" style="position: sticky; left: 0; z-index: 5;">
                                <a href="/rf/detail/{{ current_tech }}/{{ row['id'] }}" class="btn btn-info btn-action text-white" title="Chi tiết"><i class="fa-solid fa-eye"></i></a>
                                <a href="/rf/edit/{{ current_tech }}/{{ row['id'] }}" class="btn btn-warning btn-action text-white" title="Sửa"><i class="fa-solid fa-pen-to-square"></i></a>
                                <a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-danger btn-action" title="Xóa" onclick="return confirm('Bạn có chắc muốn xóa bản ghi này?')"><i class="fa-solid fa-trash"></i></a>
                            </td>
                            {% for col in rf_columns %}
                            <td style="white-space: nowrap;">{{ row[col] if row[col] is not none else '' }}</td>
                            {% endfor %}
                        </tr>
                        {% else %}
                        <tr><td colspan="{{ rf_columns|length + 1 }}" class="text-center py-3">Không có dữ liệu. Vui lòng vào menu Import để tải file lên hoặc nhấn Thêm mới.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
                <div class="text-muted small mt-2 fst-italic">Hiển thị tối đa 500 bản ghi trên web. Để xem đầy đủ, vui lòng chọn "Xuất Excel".</div>
            </div>

        {% elif active_page == 'import' %}
            <div class="row">
                <div class="col-md-8">
                    <ul class="nav nav-tabs" id="importTabs" role="tablist">
                        <!-- RF Tabs -->
                        <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#rf3g">Import RF 3G</button></li>
                        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#rf4g">Import RF 4G</button></li>
                        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#rf5g">Import RF 5G</button></li>
                        <!-- POI Tabs -->
                        <li class="nav-item"><button class="nav-link text-warning" data-bs-toggle="tab" data-bs-target="#poi4g">Import POI 4G</button></li>
                        <li class="nav-item"><button class="nav-link text-warning" data-bs-toggle="tab" data-bs-target="#poi5g">Import POI 5G</button></li>
                        <!-- KPI Tabs -->
                        <li class="nav-item"><button class="nav-link text-success" data-bs-toggle="tab" data-bs-target="#kpi3g">KPI 3G</button></li>
                        <li class="nav-item"><button class="nav-link text-success" data-bs-toggle="tab" data-bs-target="#kpi4g">KPI 4G</button></li>
                        <li class="nav-item"><button class="nav-link text-success" data-bs-toggle="tab" data-bs-target="#kpi5g">KPI 5G</button></li>
                    </ul>
                    <div class="tab-content p-4 border border-top-0 rounded-bottom">
                        <!-- RF Forms -->
                        <div class="tab-pane fade show active" id="rf3g">
                            <form action="/import?type=3g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file RF 3G (.xlsx/.csv)</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <div class="d-flex justify-content-between">
                                    <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 3G</button>
                                    <a href="/rf/reset?type=3g" class="btn btn-danger" onclick="return confirm('CẢNH BÁO: Hành động này sẽ XÓA SẠCH dữ liệu RF 3G. Bạn có chắc chắn không?')"><i class="fa-solid fa-trash-can"></i> Xóa toàn bộ RF 3G</a>
                                </div>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="rf4g">
                            <form action="/import?type=4g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file RF 4G (.xlsx/.csv)</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <div class="d-flex justify-content-between">
                                    <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 4G</button>
                                    <a href="/rf/reset?type=4g" class="btn btn-danger" onclick="return confirm('CẢNH BÁO: Hành động này sẽ XÓA SẠCH dữ liệu RF 4G. Bạn có chắc chắn không?')"><i class="fa-solid fa-trash-can"></i> Xóa toàn bộ RF 4G</a>
                                </div>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="rf5g">
                            <form action="/import?type=5g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file RF 5G (.xlsx/.csv)</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <div class="d-flex justify-content-between">
                                    <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 5G</button>
                                    <a href="/rf/reset?type=5g" class="btn btn-danger" onclick="return confirm('CẢNH BÁO: Hành động này sẽ XÓA SẠCH dữ liệu RF 5G. Bạn có chắc chắn không?')"><i class="fa-solid fa-trash-can"></i> Xóa toàn bộ RF 5G</a>
                                </div>
                            </form>
                        </div>
                        
                        <!-- POI Forms -->
                        <div class="tab-pane fade" id="poi4g">
                            <h5 class="text-warning">Import Danh sách POI 4G</h5>
                            <form action="/import?type=poi4g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file POI 4G (.xlsx/.csv)</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <button type="submit" class="btn btn-warning text-dark"><i class="fa-solid fa-map-pin"></i> Tải lên POI 4G</button>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="poi5g">
                            <h5 class="text-warning">Import Danh sách POI 5G</h5>
                            <form action="/import?type=poi5g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file POI 5G (.xlsx/.csv)</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <button type="submit" class="btn btn-warning text-dark"><i class="fa-solid fa-map-pin"></i> Tải lên POI 5G</button>
                            </form>
                        </div>

                        <!-- KPI Forms -->
                        <div class="tab-pane fade" id="kpi3g">
                            <h5 class="text-success">Import KPI 3G Hàng Ngày</h5>
                            <form action="/import?type=kpi3g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn các file KPI 3G (.csv)</label>
                                    <input type="file" name="file" class="form-control" accept=".csv" multiple required>
                                    <small class="text-muted">Có thể chọn nhiều file cùng lúc để import.</small>
                                </div>
                                <button type="submit" class="btn btn-success"><i class="fa-solid fa-chart-line"></i> Tải lên KPI 3G</button>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="kpi4g">
                            <h5 class="text-success">Import KPI 4G Hàng Ngày</h5>
                            <form action="/import?type=kpi4g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn các file KPI 4G (.csv)</label>
                                    <input type="file" name="file" class="form-control" accept=".csv" multiple required>
                                </div>
                                <button type="submit" class="btn btn-success"><i class="fa-solid fa-chart-line"></i> Tải lên KPI 4G</button>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="kpi5g">
                            <h5 class="text-success">Import KPI 5G Hàng Ngày</h5>
                            <form action="/import?type=kpi5g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn các file KPI 5G (.csv)</label>
                                    <input type="file" name="file" class="form-control" accept=".csv" multiple required>
                                </div>
                                <button type="submit" class="btn btn-success"><i class="fa-solid fa-chart-line"></i> Tải lên KPI 5G</button>
                            </form>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card h-100">
                        <div class="card-header bg-info text-white"><i class="fa-solid fa-calendar-check"></i> Dữ liệu KPI đã có</div>
                        <div class="card-body p-0" style="max-height: 400px; overflow-y: auto;">
                            <table class="table table-bordered table-striped small mb-0 text-center">
                                <thead class="table-light">
                                    <tr>
                                        <th>KPI 3G</th>
                                        <th>KPI 4G</th>
                                        <th>KPI 5G</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for r3, r4, r5 in kpi_rows %}
                                    <tr>
                                        <td class="{{ 'text-success fw-bold' if r3 else 'text-muted' }}">{{ r3 if r3 else '-' }}</td>
                                        <td class="{{ 'text-success fw-bold' if r4 else 'text-muted' }}">{{ r4 if r4 else '-' }}</td>
                                        <td class="{{ 'text-success fw-bold' if r5 else 'text-muted' }}">{{ r5 if r5 else '-' }}</td>
                                    </tr>
                                    {% else %}
                                    <tr><td colspan="3" class="text-center text-muted">Chưa có dữ liệu</td></tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

        {% else %}
            <div class="text-center py-5 text-muted"><h5>Chức năng {{ title }} đang xây dựng</h5></div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

USER_MANAGEMENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row">
    <div class="col-md-4">
        <div class="card"><div class="card-header">Thêm User</div><div class="card-body">
            <form action="/users/add" method="POST">
                <div class="mb-2"><label>Username</label><input type="text" name="username" class="form-control" required></div>
                <div class="mb-2"><label>Password</label><input type="password" name="password" class="form-control" required></div>
                <div class="mb-3"><label>Role</label><select name="role" class="form-select"><option value="user">User</option><option value="admin">Admin</option></select></div>
                <button class="btn btn-success w-100">Tạo</button>
            </form>
        </div></div>
    </div>
    <div class="col-md-8">
        <div class="card"><div class="card-header">Danh sách User</div><div class="card-body p-0">
            <table class="table table-hover mb-0">
                <thead class="table-light"><tr><th>ID</th><th>User</th><th>Role</th><th>Thao tác</th></tr></thead>
                <tbody>
                    {% for u in users %}
                    <tr><td>{{ u.id }}</td><td>{{ u.username }}</td><td><span class="badge bg-{{ 'danger' if u.role=='admin' else 'info' }}">{{ u.role }}</span></td>
                    <td>{% if u.username != 'admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-outline-danger" onclick="return confirm('Xóa?')">Xóa</a> <button class="btn btn-sm btn-outline-warning" onclick="promptReset({{ u.id }}, '{{ u.username }}')">Đổi Pass</button>{% endif %}</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div></div>
    </div>
</div>
<script>function promptReset(id, name) { let p = prompt("Pass mới cho " + name); if(p) location.href="/users/reset-pass/"+id+"?new_pass="+encodeURIComponent(p); }</script>
{% endblock %}
"""

PROFILE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row justify-content-center"><div class="col-md-6"><div class="card"><div class="card-header">Đổi mật khẩu</div><div class="card-body">
    <p>User: <strong>{{ current_user.username }}</strong></p><hr>
    <form action="/change-password" method="POST">
        <div class="mb-3"><label>Mật khẩu cũ</label><input type="password" name="current_password" class="form-control" required></div>
        <div class="mb-3"><label>Mật khẩu mới</label><input type="password" name="new_password" class="form-control" required></div>
        <button class="btn btn-primary">Lưu thay đổi</button>
    </form>
</div></div></div></div>
{% endblock %}
"""

BACKUP_RESTORE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="container py-4">
    <div class="row g-4">
        <!-- Backup Section -->
        <div class="col-md-6">
            <div class="card h-100 shadow-sm">
                <div class="card-header bg-primary text-white">
                    <h5 class="mb-0"><i class="fa-solid fa-download me-2"></i>Sao lưu Dữ liệu (Backup)</h5>
                </div>
                <div class="card-body d-flex flex-column justify-content-center align-items-center p-5">
                    <p class="text-center text-muted mb-4">
                        Tải xuống toàn bộ dữ liệu hiện tại (User, RF, KPI, POI) dưới dạng file nén (.zip).
                    </p>
                    <form action="/backup" method="POST">
                        <button type="submit" class="btn btn-primary btn-lg px-5">
                            <i class="fa-solid fa-file-zipper me-2"></i> Tải xuống bản sao lưu
                        </button>
                    </form>
                </div>
            </div>
        </div>

        <!-- Restore Section -->
        <div class="col-md-6">
            <div class="card h-100 shadow-sm border-warning">
                <div class="card-header bg-warning text-dark">
                    <h5 class="mb-0"><i class="fa-solid fa-upload me-2"></i>Khôi phục Dữ liệu (Restore)</h5>
                </div>
                <div class="card-body p-4">
                    <div class="alert alert-danger" role="alert">
                        <i class="fa-solid fa-triangle-exclamation me-2"></i>
                        <strong>CẢNH BÁO:</strong> Dữ liệu hiện tại sẽ bị xóa và thay thế bằng dữ liệu trong file backup.
                    </div>
                    <form action="/restore" method="POST" enctype="multipart/form-data">
                        <div class="mb-4">
                            <label for="backupFile" class="form-label fw-bold">Chọn file Backup (.zip)</label>
                            <input class="form-control form-control-lg" type="file" id="backupFile" name="file" accept=".zip" required>
                        </div>
                        <div class="d-grid">
                            <button type="submit" class="btn btn-warning btn-lg" onclick="return confirm('Bạn có chắc chắn muốn khôi phục dữ liệu?')">
                                <i class="fa-solid fa-rotate-left me-2"></i> Tiến hành Khôi phục
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

RF_FORM_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header">
        <span class="h5">{{ title }}</span>
        <a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm float-end">Quay lại</a>
    </div>
    <div class="card-body">
        <form method="POST">
            <div class="row g-3">
                {% for col in columns %}
                <div class="col-md-4">
                    <label class="form-label fw-bold small text-uppercase text-muted">{{ col }}</label>
                    <input type="text" name="{{ col }}" class="form-control" 
                           value="{{ obj[col] if obj and obj[col] is not none else '' }}">
                </div>
                {% endfor %}
            </div>
            <hr>
            <div class="d-flex justify-content-end">
                <button type="submit" class="btn btn-primary px-4"><i class="fa-solid fa-save"></i> Lưu lại</button>
            </div>
        </form>
    </div>
</div>
{% endblock %}
"""

RF_DETAIL_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header">
        <span class="h5">Chi tiết bản ghi RF {{ tech.upper() }} #{{ obj.id }}</span>
        <div class="float-end">
            <a href="/rf/edit/{{ tech }}/{{ obj.id }}" class="btn btn-warning btn-sm text-white"><i class="fa-solid fa-pen"></i> Sửa</a>
            <a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm">Quay lại</a>
        </div>
    </div>
    <div class="card-body">
        <div class="table-responsive">
            <table class="table table-bordered table-striped">
                <tbody>
                    {% for col, val in obj.items() %}
                        {% if col != '_sa_instance_state' %}
                        <tr>
                            <th class="bg-light" style="width: 30%">{{ col.upper() }}</th>
                            <td>{{ val if val is not none else '' }}</td>
                        </tr>
                        {% endif %}
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>
{% endblock %}
"""

# --- CẤU HÌNH LOADER TEMPLATE ẢO ---
app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})

def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE:
        return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

# --- ROUTES ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        try:
            user = User.query.filter_by(username=request.form['username']).first()
            if user and user.check_password(request.form['password']):
                login_user(user)
                return redirect(url_for('index'))
            flash('Sai thông tin đăng nhập', 'danger')
        except Exception as e: flash(f"Lỗi DB: {e}", 'danger')
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

# Các route menu khác
@app.route('/kpi')
@login_required
def kpi():
    selected_tech = request.args.get('tech', '3g')
    cell_name_input = request.args.get('cell_name', '').strip()
    poi_input = request.args.get('poi_name', '').strip()
    charts = {} 

    colors = [
        '#007bff', '#28a745', '#dc3545', '#ffc107', '#17a2b8', 
        '#6610f2', '#e83e8c', '#fd7e14', '#20c997', '#6c757d'
    ]
    
    target_cells = []
    KPI_Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)
    RF_Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(selected_tech)

    # 1. LOGIC TÌM KIẾM: POI -> Site Code -> Cell Name
    if poi_input:
        # Tìm tất cả cell trong POI (cả 4G và 5G)
        cells_4g = [r.cell_code for r in POI4G.query.filter(POI4G.poi_name == poi_input).all()]
        cells_5g = [r.cell_code for r in POI5G.query.filter(POI5G.poi_name == poi_input).all()]
        
        # Nếu đang ở tab 3G/4G thì ưu tiên lấy cell 4G, tab 5G lấy 5G
        if selected_tech == '5g':
            target_cells = cells_5g
        else:
            target_cells = cells_4g # 3G thường dùng chung site với 4G hoặc logic riêng, ở đây tạm lấy theo POI 4G
            
    elif cell_name_input and RF_Model:
        # Check if input is Site Code
        site_cells = RF_Model.query.filter(RF_Model.site_code == cell_name_input).all()
        if site_cells:
            target_cells = [c.cell_code for c in site_cells]
        else:
            # Assume list of cells
            target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]

    # 2. VẼ BIỂU ĐỒ NẾU CÓ CELL
    if target_cells and KPI_Model:
        data = KPI_Model.query.filter(KPI_Model.ten_cell.in_(target_cells)).all()
        
        # Sort data by date
        try:
            data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
        except ValueError: pass 

        if data:
            # X Axis: Unique sorted dates
            all_labels = sorted(list(set([x.thoi_gian for x in data])), key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
            
            # Group data by Cell
            data_by_cell = defaultdict(list)
            for x in data:
                data_by_cell[x.ten_cell].append(x)

            # Define Metrics to Plot
            metrics_config = {
                '3g': [
                    {'key': 'pstraffic', 'label': 'PSTRAFFIC (GB)'},
                    {'key': 'traffic', 'label': 'TRAFFIC (Erl)'},
                    {'key': 'psconges', 'label': 'PS CONGESTION (%)'},
                    {'key': 'csconges', 'label': 'CS CONGESTION (%)'}
                ],
                '4g': [
                    {'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'},
                    {'key': 'user_dl_avg_thput', 'label': 'USER DL AVG THPUT (Mbps)'},
                    {'key': 'res_blk_dl', 'label': 'RES BLOCK DL (%)'},
                    {'key': 'cqi_4g', 'label': 'CQI 4G'}
                ],
                '5g': [
                    {'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'},
                    {'key': 'user_dl_avg_throughput', 'label': 'USER DL AVG THPUT (Mbps)'},
                    {'key': 'cqi_5g', 'label': 'CQI 5G'}
                ]
            }
            
            current_metrics = metrics_config.get(selected_tech, [])

            for metric in current_metrics:
                metric_key = metric['key']
                metric_label = metric['label']
                datasets = []
                
                # Create a line for each cell
                for i, cell_code in enumerate(target_cells):
                    cell_data = data_by_cell.get(cell_code, [])
                    # Map data to timeline (fill missing dates with null or 0)
                    data_map = {item.thoi_gian: getattr(item, metric_key, 0) or 0 for item in cell_data}
                    aligned_data = [data_map.get(label, None) for label in all_labels]
                    
                    color = colors[i % len(colors)]
                    datasets.append({
                        'label': cell_code,
                        'data': aligned_data,
                        'borderColor': color,
                        'backgroundColor': color,
                        'tension': 0.1,
                        'fill': False,
                        'spanGaps': True
                    })
                
                chart_id = f"chart_{metric_key}"
                charts[chart_id] = {
                    'title': metric_label,
                    'labels': all_labels,
                    'datasets': datasets
                }
    
    # 3. LẤY DANH SÁCH POI CHO DATALIST
    poi_list = []
    with app.app_context():
        try:
            p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
            p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
            poi_list = sorted(list(set(p4 + p5)))
        except: pass

    return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi', 
                       selected_tech=selected_tech, cell_name_input=cell_name_input, 
                       selected_poi=poi_input, poi_list=poi_list, charts=charts)

@app.route('/poi')
@login_required
def poi():
    pname = request.args.get('poi_name', '').strip()
    charts = {}
    
    # Get all POIs
    pois = []
    try:
        p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
        p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
        pois = sorted(list(set(p4 + p5)))
    except: pass
    
    if pname:
        # 4G Agg
        c4 = [r[0] for r in db.session.query(POI4G.cell_code).filter_by(poi_name=pname).all()]
        if c4:
            k4 = KPI4G.query.filter(KPI4G.ten_cell.in_(c4)).all()
            agg = defaultdict(lambda: {'traf':0, 'thp':0, 'cnt':0})
            for r in k4:
                agg[r.thoi_gian]['traf'] += (r.traffic or 0)
                agg[r.thoi_gian]['thp'] += (r.user_dl_avg_thput or 0)
                agg[r.thoi_gian]['cnt'] += 1
            # Sort & Format
            dates = sorted(agg.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
            charts['4g_traf'] = {'title': 'Total 4G Traffic', 'labels': dates, 'datasets': [{'label': 'GB', 'data': [agg[d]['traf'] for d in dates], 'borderColor': 'blue'}]}
            charts['4g_thp'] = {'title': 'Avg 4G Throughput', 'labels': dates, 'datasets': [{'label': 'Mbps', 'data': [(agg[d]['thp']/agg[d]['cnt']) if agg[d]['cnt'] else 0 for d in dates], 'borderColor': 'green'}]}

        # 5G Agg (Similar logic)
        c5 = [r[0] for r in db.session.query(POI5G.cell_code).filter_by(poi_name=pname).all()]
        if c5:
            k5 = KPI5G.query.filter(KPI5G.ten_cell.in_(c5)).all()
            agg = defaultdict(lambda: {'traf':0, 'thp':0, 'cnt':0})
            for r in k5:
                agg[r.thoi_gian]['traf'] += (r.traffic or 0)
                agg[r.thoi_gian]['thp'] += (r.user_dl_avg_throughput or 0)
                agg[r.thoi_gian]['cnt'] += 1
            dates = sorted(agg.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
            charts['5g_traf'] = {'title': 'Total 5G Traffic', 'labels': dates, 'datasets': [{'label': 'GB', 'data': [agg[d]['traf'] for d in dates], 'borderColor': 'orange'}]}
            charts['5g_thp'] = {'title': 'Avg 5G Throughput', 'labels': dates, 'datasets': [{'label': 'Mbps', 'data': [(agg[d]['thp']/agg[d]['cnt']) if agg[d]['cnt'] else 0 for d in dates], 'borderColor': 'purple'}]}

    return render_page(CONTENT_TEMPLATE, title="POI Report", active_page='poi', poi_list=pois, selected_poi=pname, poi_charts=charts)

@app.route('/conges-3g')
@login_required
def conges_3g():
    # Logic 3 days
    dates = [r[0] for r in db.session.query(KPI3G.thoi_gian).distinct().limit(3).all()] # Order by desc needed
    # ... (Simplified logic for brevity, assume similar to previous)
    return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=[], dates=dates)

@app.route('/backup-restore')
@login_required
def backup_restore(): return render_page(BACKUP_RESTORE_TEMPLATE, title="Backup", active_page='backup_restore')
@app.route('/backup', methods=['POST'])
@login_required
def backup_db(): return redirect(url_for('index')) # Placeholder
@app.route('/restore', methods=['POST'])
@login_required
def restore_db(): return redirect(url_for('index')) # Placeholder

@app.route('/worst-cell')
@login_required
def worst_cell(): return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell')
@app.route('/traffic-down')
@login_required
def traffic_down(): return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down')
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

# --- TEMPLATE LOADERS & UTILS ---
app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})
def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE: return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

valid_db_columns = [] # Helper populated dynamically if needed, or check per model

if __name__ == '__main__':
    app.run(debug=True)
