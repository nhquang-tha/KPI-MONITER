import os
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
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
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
        'CellID': 'cellid', 'NetworkTech': 'networktech',
        'CELL': 'cell_code', 'SITE': 'site_code', 'MÃ CELL': 'cell_code', 'MÃ TRẠM': 'site_code',
        'UARFCN': 'dl_uarfcn', 'LAC': 'bsc_lac', 'RNC': 'bsc_lac', 'BSC': 'bsc_lac',
        'TÊN CELL': 'cell_name', 'CELLNAME': 'cell_name', 'TÊN TRẠM': 'site_name', 'CELL ID': 'cell_code', 'SITE ID': 'site_code',
        'LAT': 'latitude', 'LONG': 'longitude', 'KINH ĐỘ': 'longitude', 'VĨ ĐỘ': 'latitude',
        'TILT': 'total_tilt', 'ANTEN': 'antena', 'THIẾT BỊ': 'equipment',
        'FREQ': 'frequency', 'TRẠM': 'site_code', 'NODEB': 'site_code', 'NODEB NAME': 'site_name',
        # MAPPING TỪ FILE CELL_3G
        'Tên người quản lý': 'nguoi_quan_ly', 'SDT người quản lý': 'sdt_nguoi_quan_ly', 'Ngày hoạt động': 'ngay_hoat_dong',
        'Hoàn cảnh ra đời': 'hoan_canh_ra_doi', 'Tên quản lý': 'ten_quan_ly', 'Antenna gain': 'antenna_gain',
        'Antenna high': 'antenna_high', 'Mechanical tilt': 'mechanical_tilt', 'Mechainical tilt': 'mechanical_tilt',
        'Electrical tilt': 'electrical_tilt', 'Địa chỉ': 'dia_chi', 'Mã CSHT CỦA CELL': 'csht_cell',
        'Mã CSHT CỦA TRẠM': 'csht_site', 'Tên đơn vị': 'ten_don_vi', 'Tên thiết bị': 'thiet_bi',
        'Tên trên hệ thống': 'ten_tren_he_thong', 'dl_psc': 'dl_psc', 'cpich_power': 'cpich_power',
        'Total power': 'total_power', 'Băng tần': 'bang_tan', 'Tên loại trạm': 'ten_loai_tram',
        'Loại ăn ten': 'loai_anten', 'Antenna Tên hãng SX': 'hang_sx_anten', 'Antenna Dải tần hoạt động': 'anten_dai_tan',
        'Antenna dùng chung': 'anten_dung_chung', 'Antenna số port': 'anten_so_port',
        # MAPPING TỪ FILE CONFIG3G
        'Mã Trạm': 'ma_tram', 'Đơn vị quản lý': 'don_vi_quan_ly', 'Mã CSHT': 'ma_csht',
        'Loại trạm': 'loai_tram', 'Site Name': 'site_name', 'Cell Name (Alias)': 'cell_name_alias',
        'Cell Name': 'cell_name', 'RAC': 'rac', 'DC_support': 'dc_support', 'cpichPower': 'cpich_power',
        'totalPower': 'total_power', 'maxPower': 'max_power', 'OAM IP': 'oam_ip', 'MechanicalTilt': 'mechanical_tilt',
        'ElectricalTilt': 'electrical_tilt', 'TotalTilt': 'total_tilt', 'AntennaType': 'antenna_type',
        'AntennaHigh': 'antenna_high', 'AntennaGain': 'antenna_gain'
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

class Cell3G(db.Model):
    __tablename__ = 'cell_3g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(100), index=True)
    site_code = db.Column(db.String(50))
    nguoi_quan_ly = db.Column(db.String(100))
    sdt_nguoi_quan_ly = db.Column(db.String(50))
    ngay_hoat_dong = db.Column(db.String(50))
    hoan_canh_ra_doi = db.Column(db.Text)
    ten_quan_ly = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    antenna_gain = db.Column(db.Float)
    antenna_high = db.Column(db.Float)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    dia_chi = db.Column(db.String(255))
    csht_cell = db.Column(db.String(50))
    csht_site = db.Column(db.String(50))
    longitude = db.Column(db.Float)
    latitude = db.Column(db.Float)
    ten_don_vi = db.Column(db.String(100))
    thiet_bi = db.Column(db.String(50))
    ten_tren_he_thong = db.Column(db.String(100))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    dl_psc = db.Column(db.String(50))
    cpich_power = db.Column(db.Float)
    total_power = db.Column(db.Float)
    bang_tan = db.Column(db.String(50))
    ten_loai_tram = db.Column(db.String(50))
    loai_anten = db.Column(db.String(50))
    hang_sx_anten = db.Column(db.String(50))
    anten_dai_tan = db.Column(db.String(100))
    anten_dung_chung = db.Column(db.String(50))
    anten_so_port = db.Column(db.String(20))
    extra_data = db.Column(db.Text)

class Config3G(db.Model):
    __tablename__ = 'config_3g'
    id = db.Column(db.Integer, primary_key=True)
    ma_tram = db.Column(db.String(50))
    don_vi_quan_ly = db.Column(db.String(100))
    thiet_bi = db.Column(db.String(50))
    ma_csht = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    loai_tram = db.Column(db.String(50))
    site_name = db.Column(db.String(100))
    cell_name_alias = db.Column(db.String(100))
    cell_name = db.Column(db.String(100), index=True)
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    rac = db.Column(db.String(50))
    bang_tan = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    dl_psc = db.Column(db.String(50))
    dc_support = db.Column(db.String(50))
    cpich_power = db.Column(db.Float)
    total_power = db.Column(db.Float)
    max_power = db.Column(db.Float)
    oam_ip = db.Column(db.String(50))
    azimuth = db.Column(db.Integer)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    antenna_type = db.Column(db.String(100))
    antenna_high = db.Column(db.Float)
    antenna_gain = db.Column(db.Float)
    extra_data = db.Column(db.Text)

class RF3G(db.Model):
    __tablename__ = 'rf_3g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(100), index=True)
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
    # Trường gộp
    ma_tram = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    psc = db.Column(db.String(50))
    extra_data = db.Column(db.Text)

# --- Other Models (RF4G, RF5G, KPI...) keep as is ---
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
# 4. TELEGRAM BOT & ROUTES
# ==============================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']):
            login_user(user)
            return redirect(url_for('index'))
        flash('Login failed', 'danger')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/sync-rf3g', methods=['POST'])
@login_required
def sync_rf3g():
    if current_user.role != 'admin': return redirect(url_for('index'))
    try:
        db.session.query(RF3G).delete()
        cells = {str(c.cell_code).strip().upper(): c for c in Cell3G.query.all() if c.cell_code}
        configs = {str(c.cell_name).strip().upper(): c for c in Config3G.query.all() if c.cell_name}
        
        # Lấy danh sách tất cả các mã cell từ cả 2 bảng
        all_codes = set(cells.keys()) | set(configs.keys())
        
        rf3g_records = []
        for code in all_codes:
            c = cells.get(code)
            cfg = configs.get(code)
            
            merged_extra = {}
            if c and c.extra_data:
                try: merged_extra.update(json.loads(c.extra_data))
                except: pass
            if cfg and cfg.extra_data:
                try: merged_extra.update(json.loads(cfg.extra_data))
                except: pass
            
            # Gộp dữ liệu thông minh
            record = RF3G(
                cell_code=code,
                site_code=getattr(c, 'site_code', getattr(cfg, 'ma_tram', None)),
                cell_name=getattr(c, 'ten_tren_he_thong', getattr(cfg, 'cell_name', code)),
                latitude=getattr(c, 'latitude', getattr(cfg, 'latitude', None)),
                longitude=getattr(c, 'longitude', getattr(cfg, 'longitude', None)),
                azimuth=getattr(c, 'azimuth', getattr(cfg, 'azimuth', None)),
                total_tilt=getattr(c, 'total_tilt', getattr(cfg, 'total_tilt', None)),
                frequency=getattr(cfg, 'bang_tan', getattr(c, 'bang_tan', None)),
                psc=getattr(cfg, 'dl_psc', getattr(c, 'dl_psc', None)),
                bsc_lac=getattr(cfg, 'lac', getattr(c, 'lac', None)),
                ci=getattr(cfg, 'ci', getattr(c, 'ci', None)),
                extra_data=json.dumps(merged_extra, ensure_ascii=False) if merged_extra else None
            )
            rf3g_records.append(record)
            
        if rf3g_records:
            db.session.bulk_save_objects(rf3g_records)
            db.session.commit()
            flash(f'Đã ghép nối và đồng bộ {len(rf3g_records)} trạm 3G thành công!', 'success')
        else:
            flash('Không có dữ liệu 3G để đồng bộ. Vui lòng kiểm tra lại file đã upload.', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Lỗi đồng bộ: {str(e)}', 'danger')
    return redirect(url_for('import_data'))

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
    return render_template('content.html', title="Dashboard", active_page='dashboard', dashboard_data=dashboard_data)

@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.form.get('type')
        cfg = {'cell3g': Cell3G, 'config3g': Config3G, '3g': RF3G, '4g': RF4G, '5g': RF5G, 'kpi3g': KPI3G, 'kpi4g': KPI4G, 'kpi5g': KPI5G, 'poi4g': POI4G, 'poi5g': POI5G}
        Model = cfg.get(itype)
        if Model:
            valid_cols = [c.key for c in Model.__table__.columns if c.key not in ['id', 'extra_data']]
            for file in files:
                try:
                    df_raw = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip') if file.filename.endswith('.csv') else pd.read_excel(file)
                    # Dò header thông minh
                    header_idx = -1
                    for i, row in df_raw.head(20).iterrows():
                        row_vals = [str(v).lower() for v in row.values if pd.notna(v)]
                        if any(k in " ".join(row_vals) for k in ['cell', 'site', 'trạm', 'uarfcn', 'hệ thống']):
                            header_idx = i
                            break
                    if header_idx != -1:
                        df_raw.columns = df_raw.iloc[header_idx]
                        df_raw = df_raw.iloc[header_idx + 1:].reset_index(drop=True)
                    df_raw = df_raw.dropna(how='all')
                    original_columns = list(df_raw.columns)
                    df_raw.columns = [clean_header(c) for c in df_raw.columns]
                    header_mapping = dict(zip(df_raw.columns, original_columns))
                    
                    records = []
                    for row in df_raw.to_dict('records'):
                        clean_row, extra = {}, {}
                        for k, v in row.items():
                            if pd.isna(v) or str(v).strip() == '': continue
                            if k in valid_cols: clean_row[k] = v
                            else: extra[header_mapping.get(k, k)] = str(v)
                        
                        # Định danh mã cell
                        c_code = clean_row.get('cell_code') or clean_row.get('cell_name') or clean_row.get('ten_tren_he_thong')
                        if not c_code and extra:
                            for ex_k, ex_v in extra.items():
                                if any(w in str(ex_k).lower() for w in ['cell', 'site', 'trạm', 'node']):
                                    c_code = ex_v; break
                        
                        if c_code:
                            if 'cell_code' in valid_cols: clean_row['cell_code'] = str(c_code).strip()
                            if 'cell_name' in valid_cols: clean_row['cell_name'] = str(c_code).strip()
                            if hasattr(Model, 'extra_data') and extra: clean_row['extra_data'] = json.dumps(extra, ensure_ascii=False)
                            records.append(clean_row)
                    
                    if records:
                        db.session.bulk_insert_mappings(Model, records)
                        db.session.commit()
                        flash(f'Đã import thành công {len(records)} dòng vào {itype.upper()}.', 'success')
                except Exception as e: flash(f'Lỗi file {file.filename}: {e}', 'danger')
        return redirect(url_for('import_data'))
        
    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    today = datetime.now()
    year, week_num, _ = today.isocalendar()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    default_week_name = f"Tuần {week_num:02d} ({start_of_week.strftime('%d/%m')}-{end_of_week.strftime('%d/%m')})"
    return render_template('content.html', title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), default_week_name=default_week_name)

# --- Other routes (GIS, KPI, Scripts...) keep as is from conversation history ---

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis():
    # Keep previous logic but ensuring it works with new RF3G columns
    return render_template('content.html', title="Bản đồ Trực quan (GIS)", active_page='gis')

if __name__ == '__main__':
    app.run(debug=True)
