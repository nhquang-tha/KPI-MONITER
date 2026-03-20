import os
import jinja2
import pandas as pd
import json
import gc
import re
import zipfile
import random
import math
import requests
import urllib.parse
from io import BytesIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect, or_
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
        'UL Traffic Volume (GB)': 'ul_traffic_volume_gb', 'DL Traffic Volume (GB)': 'dl_traffic_volume_gb',
        'Total Data Traffic Volume (GB)': 'traffic', 'Cell Uplink Average Throughput': 'cell_uplink_average_throughput',
        'Cell Downlink Average Throughput': 'cell_downlink_average_throughput', 'A User Downlink Average Throughput': 'user_dl_avg_throughput',
        'Cell avaibility rate': 'cell_avaibility_rate', 'SgNB Addition Success Rate': 'sgnb_addition_success_rate',
        'SgNB Abnormal Release Rate': 'sgnb_abnormal_release_rate', 'CQI_5G': 'cqi_5g', 'CQI_4G': 'cqi_4g',
        'POI': 'poi_name', 'Cell_Code': 'cell_code', 'Site_Code': 'site_code', 'CSHT_code': 'csht_code',
        'Hãng_SX': 'hang_sx', 'Antena': 'antena', 'Swap': 'swap', 'Start_day': 'start_day', 'Ghi_chú': 'ghi_chu',
        'Anten_height': 'anten_height', 'Azimuth': 'azimuth', 'M_T': 'm_t', 'E_T': 'e_t', 'Total_tilt': 'total_tilt',
        'PSC': 'psc', 'DL_UARFCN': 'dl_uarfcn', 'BSC_LAC': 'bsc_lac', 'CI': 'ci',
        'Latitude': 'latitude', 'Longitude': 'longitude', 'Equipment': 'equipment', 'nrarfcn': 'nrarfcn',
        'Lcrid': 'lcrid', 'Đồng_bộ': 'dong_bo', 'CellID': 'cellid', 'NetworkTech': 'networktech',
        'CELL': 'cell_code', 'SITE': 'site_code', 'MÃ CELL': 'cell_code', 'MÃ TRẠM': 'site_code',
        'UARFCN': 'dl_uarfcn', 'LAC': 'bsc_lac', 'RNC': 'bsc_lac', 'BSC': 'bsc_lac',
        'TÊN CELL': 'cell_name', 'CELLNAME': 'cell_name', 'TÊN TRẠM': 'site_name', 'CELL ID': 'cell_code', 'SITE ID': 'site_code',
        'LAT': 'latitude', 'LONG': 'longitude', 'KINH ĐỘ': 'longitude', 'VĨ ĐỘ': 'latitude',
        'TILT': 'total_tilt', 'ANTEN': 'antena', 'THIẾT BỊ': 'equipment', 'FREQ': 'frequency',
        'TRẠM': 'site_code', 'NODEB': 'site_code', 'NODEB NAME': 'site_name',
        # MAPPING TỪ HÌNH 1 & HÌNH 2
        'STT': 'stt', 'Mã Node': 'ma_node', 'Site Code': 'site_code', 'Mã Cell': 'cell_code', 'Thiết bị': 'thiet_bi',
        'Tỉnh/TP': 'tinh_tp', 'Đơn vị quản lý': 'don_vi_quan_ly', 'Mã CSHT': 'ma_csht', 'Loại trạm': 'loai_tram',
        'Site Name': 'site_name', 'Cell Name': 'cell_name', 'Cell Name (Alias)': 'cell_name_alias', 'ci': 'ci',
        'lac': 'lac', 'rac': 'rac', 'Băng tần': 'bang_tan', 'dlPsc': 'dl_psc', 'DL_UARFCN': 'dl_uarfcn',
        'cpichPower': 'cpich_power', 'maxPower': 'max_power', 'totalPower': 'total_power', 'DC_support': 'dc_support',
        'OAM IP': 'oam_ip', 'MechanicalTilt': 'mechanical_tilt', 'ElectricalTilt': 'electrical_tilt', 'TotalTilt': 'total_tilt',
        'AntennaType': 'antenna_type', 'AntennaGain': 'antenna_gain', 'AntennaHigh': 'antenna_high', 'Cell Type': 'cell_type',
        'noOfCarrier': 'no_of_carrier', 'SpecialCoverage': 'special_coverage', 'Trạng thái': 'trang_thai', 'Note': 'ghi_chu',
        'Tên quản lý': 'ten_quan_ly', 'Tên người quản lý': 'ten_quan_ly', 'SDT người quản lý': 'sdt_nguoi_quan_ly',
        'Ngày hoạt động': 'ngay_hoat_dong', 'Hoàn cảnh ra đời': 'hoan_canh_ra_doi', 'Loại ăn ten': 'loai_anten',
        'Antenna Tên hãng SX': 'hang_sx_anten', 'Antenna Dải tần hoạt động': 'anten_dai_tan', 'Antenna dùng chung': 'anten_dung_chung',
        'Antenna số port': 'anten_so_port', 'Tên loại trạm': 'ten_loai_tram', 'Địa chỉ': 'dia_chi',
        'Mã CSHT CỦA TRẠM': 'csht_site', 'Mã CSHT CỦA CELL': 'csht_cell', 'Tên đơn vị': 'ten_don_vi',
        'Mechainical tilt': 'mechanical_tilt', 'Total power': 'total_power', 'Tên trên hệ thống': 'ten_tren_he_thong'
    }
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper: return val
    clean = re.sub(r'[^a-z0-9]', '_', remove_accents(col_name).lower())
    clean = re.sub(r'_+', '_', clean)
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo', 'ten_cell': 'ten_cell',
        'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap', 'traffic_vol_dl': 'traffic_vol_dl',
        'res_blk_dl': 'res_blk_dl', 'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges',
        'cs_so_att': 'cs_so_att', 'ps_so_att': 'ps_so_att', 'service_drop_all': 'service_drop_all',
        'user_dl_avg_thput': 'user_dl_avg_thput', 'poi': 'poi_name', 'cell_code': 'cell_code', 'site_code': 'site_code'
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
    stt = db.Column(db.String(20))
    ma_node = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    cell_code = db.Column(db.String(100), index=True)
    thiet_bi = db.Column(db.String(50))
    tinh_tp = db.Column(db.String(100))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    loai_anten = db.Column(db.String(100))
    hang_sx_anten = db.Column(db.String(100))
    anten_dai_tan = db.Column(db.String(100))
    anten_dung_chung = db.Column(db.String(50))
    anten_so_port = db.Column(db.String(50))
    antenna_gain = db.Column(db.Float)
    antenna_high = db.Column(db.Float)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    ten_loai_tram = db.Column(db.String(100))
    dia_chi = db.Column(db.String(255))
    csht_site = db.Column(db.String(100))
    csht_cell = db.Column(db.String(100))
    ten_don_vi = db.Column(db.String(100))
    bang_tan = db.Column(db.String(50))
    ten_quan_ly = db.Column(db.String(100))
    sdt_nguoi_quan_ly = db.Column(db.String(50))
    ngay_hoat_dong = db.Column(db.String(50))
    hoan_canh_ra_doi = db.Column(db.Text)
    trang_thai = db.Column(db.String(50))
    ghi_chu = db.Column(db.Text)
    ten_tren_he_thong = db.Column(db.String(100))
    extra_data = db.Column(db.Text)

class Config3G(db.Model):
    __tablename__ = 'config_3g'
    id = db.Column(db.Integer, primary_key=True)
    stt = db.Column(db.String(20))
    ma_node = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    cell_code = db.Column(db.String(100), index=True)
    thiet_bi = db.Column(db.String(50))
    tinh_tp = db.Column(db.String(100))
    don_vi_quan_ly = db.Column(db.String(100))
    ma_csht = db.Column(db.String(100))
    loai_tram = db.Column(db.String(50))
    site_name = db.Column(db.String(100))
    cell_name = db.Column(db.String(100))
    cell_name_alias = db.Column(db.String(100))
    ci = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    rac = db.Column(db.String(50))
    bang_tan = db.Column(db.String(50))
    dl_psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    cpich_power = db.Column(db.Float)
    max_power = db.Column(db.Float)
    total_power = db.Column(db.Float)
    dc_support = db.Column(db.String(50))
    oam_ip = db.Column(db.String(50))
    longitude = db.Column(db.Float)
    latitude = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    antenna_type = db.Column(db.String(100))
    antenna_gain = db.Column(db.Float)
    antenna_high = db.Column(db.Float)
    cell_type = db.Column(db.String(50))
    no_of_carrier = db.Column(db.String(50))
    special_coverage = db.Column(db.String(100))
    trang_thai = db.Column(db.String(50))
    ghi_chu = db.Column(db.Text)
    extra_data = db.Column(db.Text)

class RF3G(db.Model):
    __tablename__ = 'rf_3g'
    id = db.Column(db.Integer, primary_key=True)
    stt = db.Column(db.String(20))
    cell_code = db.Column(db.String(100), index=True)
    site_code = db.Column(db.String(50))
    ma_node = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    cell_name_alias = db.Column(db.String(100))
    site_name = db.Column(db.String(100))
    loai_tram = db.Column(db.String(50))
    thiet_bi = db.Column(db.String(50))
    tinh_tp = db.Column(db.String(100))
    don_vi_quan_ly = db.Column(db.String(100))
    ma_csht = db.Column(db.String(100))
    csht_site = db.Column(db.String(100))
    csht_cell = db.Column(db.String(100))
    ten_don_vi = db.Column(db.String(100))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    antenna_type = db.Column(db.String(100))
    loai_anten = db.Column(db.String(100))
    hang_sx_anten = db.Column(db.String(100))
    anten_dai_tan = db.Column(db.String(100))
    anten_dung_chung = db.Column(db.String(50))
    anten_so_port = db.Column(db.String(50))
    antenna_gain = db.Column(db.Float)
    antenna_high = db.Column(db.Float)
    bang_tan = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    rac = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    dl_psc = db.Column(db.String(50))
    cpich_power = db.Column(db.Float)
    max_power = db.Column(db.Float)
    total_power = db.Column(db.Float)
    dc_support = db.Column(db.String(50))
    oam_ip = db.Column(db.String(50))
    cell_type = db.Column(db.String(50))
    no_of_carrier = db.Column(db.String(50))
    special_coverage = db.Column(db.String(100))
    trang_thai = db.Column(db.String(50))
    ten_quan_ly = db.Column(db.String(100))
    sdt_nguoi_quan_ly = db.Column(db.String(50))
    ngay_hoat_dong = db.Column(db.String(50))
    hoan_canh_ra_doi = db.Column(db.Text)
    dia_chi = db.Column(db.String(255))
    ghi_chu = db.Column(db.Text)
    extra_data = db.Column(db.Text)

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
        try:
            inspector = inspect(db.engine)
            if 'config_3g' in inspector.get_table_names():
                existing_columns = [col['name'] for col in inspector.get_columns('config_3g')]
                if 'don_vi_quan_ly' not in existing_columns or 'cell_code' not in existing_columns:
                    print("--> Phát hiện cấu trúc bảng RF/Config 3G cũ. Tiến hành Auto-Reset Schema...")
                    db.session.execute(text("DROP TABLE IF EXISTS cell_3g"))
                    db.session.execute(text("DROP TABLE IF EXISTS config_3g"))
                    db.session.execute(text("DROP TABLE IF EXISTS rf_3g"))
                    db.session.commit()
        except Exception as e:
            print("Auto-migration check failed:", e)

        db.create_all()
        
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin123')
            db.session.add(u)
            db.session.commit()
init_database()

# ==============================================================================
# 4. TEMPLATES 
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
        :root { --acrylic-bg: rgba(255, 255, 255, 0.6); --acrylic-blur: blur(20px); --sidebar-bg: rgba(240, 240, 245, 0.85); --primary-color: #0078d4; --text-color: #212529; --shadow-soft: 0 4px 12px rgba(0, 0, 0, 0.05); --shadow-hover: 0 8px 16px rgba(0, 0, 0, 0.1); --border-radius: 12px; }
        body { background: linear-gradient(135deg, #f3f4f6 0%, #eef2f3 100%); font-family: 'Segoe UI', sans-serif; color: var(--text-color); overflow-x: hidden; }
        .sidebar { height: 100vh; width: 260px; position: fixed; top: 0; left: 0; background: var(--sidebar-bg); backdrop-filter: var(--acrylic-blur); border-right: 1px solid rgba(255,255,255,0.5); z-index: 1000; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); padding-top: 1rem; overflow-y: auto; padding-bottom: 60px; }
        .sidebar-header { padding: 1.5rem; color: var(--primary-color); font-weight: 600; font-size: 1.5rem; text-align: center; letter-spacing: 0.5px; }
        .sidebar-menu { padding: 0; list-style: none; margin: 1rem 0; }
        .sidebar-menu a { display: flex; align-items: center; padding: 14px 25px; color: #555; text-decoration: none; font-weight: 500; border-left: 4px solid transparent; transition: all 0.2s ease; margin: 4px 12px; border-radius: 8px; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background-color: rgba(255, 255, 255, 0.8); color: var(--primary-color); box-shadow: 0 2px 8px rgba(0,0,0,0.05); }
        .sidebar-menu a.active { border-left-color: var(--primary-color); background-color: rgba(255, 255, 255, 0.95); }
        .sidebar-menu i { margin-right: 15px; width: 24px; text-align: center; font-size: 1.1rem; }
        .main-content { margin-left: 260px; padding: 30px; min-height: 100vh; transition: all 0.3s ease; }
        .card { border: none; border-radius: var(--border-radius); background: rgba(255, 255, 255, 0.85); backdrop-filter: blur(10px); box-shadow: var(--shadow-soft); transition: box-shadow 0.3s ease; margin-bottom: 1.5rem; }
        .card:hover { box-shadow: var(--shadow-hover); }
        .card-header { background-color: rgba(255, 255, 255, 0.9); border-bottom: 1px solid rgba(0,0,0,0.05); padding: 1.25rem 1.5rem; font-weight: 600; color: #333; font-size: 1.1rem; }
        .card-body { padding: 1.5rem; }
        .btn-primary { background-color: var(--primary-color); border: none; box-shadow: 0 2px 6px rgba(0, 120, 212, 0.3); border-radius: 6px; padding: 0.5rem 1.25rem; font-weight: 500; transition: all 0.2s; }
        .btn-primary:hover { background-color: #0063b1; box-shadow: 0 4px 12px rgba(0, 120, 212, 0.4); transform: translateY(-1px); }
        .btn-warning { background-color: #ffaa44; border: none; box-shadow: 0 2px 6px rgba(255, 170, 68, 0.3); border-radius: 6px; padding: 0.5rem 1.25rem; font-weight: 500; color: #fff; transition: all 0.2s; }
        .btn-warning:hover { background-color: #e69532; color: #fff; box-shadow: 0 4px 12px rgba(255, 170, 68, 0.4); transform: translateY(-1px); }
        .table { background: transparent; }
        .table thead th { background-color: rgba(248, 249, 250, 0.8); border-bottom: 2px solid #e9ecef; color: #555; font-weight: 600; font-size: 0.9rem; text-transform: uppercase; }
        .table-hover tbody tr:hover { background-color: rgba(0, 120, 212, 0.05); }
        .chart-container canvas { cursor: zoom-in; }
        @media (max-width: 768px) { .sidebar { margin-left: -260px; } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; padding: 15px; } }
        .legend { line-height: 18px; color: #333; }
        .legend i { width: 14px; height: 14px; float: left; margin-right: 8px; opacity: 0.8; border: 1px solid #999; }
        .table-responsive::-webkit-scrollbar { height: 8px; width: 8px; }
        .table-responsive::-webkit-scrollbar-track { background: #f1f1f1; border-radius: 4px; }
        .table-responsive::-webkit-scrollbar-thumb { background: #c1c1c1; border-radius: 4px; }
        .table-responsive::-webkit-scrollbar-thumb:hover { background: #a8a8a8; }
        #sidebar-overlay { display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.4); z-index: 999; backdrop-filter: blur(2px); transition: all 0.3s ease; }
        #sidebar-overlay.active { display: block; }
    </style>
</head>
<body>
    <div id="sidebar-overlay" onclick="toggleSidebar()"></div>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired"></i> NetOps</div>
        <ul class="sidebar-menu">
            <li><a href="{{ url_for('index') }}" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="{{ url_for('gis') }}" class="{{ 'active' if active_page == 'gis' else '' }}"><i class="fa-solid fa-map-location-dot"></i> Bản đồ GIS</a></li>
            <li><a href="{{ url_for('kpi') }}" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="{{ url_for('qoe_qos') }}" class="{{ 'active' if active_page == 'qoe_qos' else '' }}"><i class="fa-solid fa-star-half-stroke"></i> QoE QoS Analytics</a></li>
            <li><a href="{{ url_for('optimize') }}" class="{{ 'active' if active_page == 'optimize' else '' }}"><i class="fa-solid fa-wand-magic-sparkles"></i> Tối ưu QoE/QoS</a></li>
            <li><a href="{{ url_for('rf') }}" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="{{ url_for('poi') }}" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-pin"></i> POI Report</a></li>
            <li><a href="{{ url_for('worst_cell') }}" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="{{ url_for('conges_3g') }}" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="{{ url_for('traffic_down') }}" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li>
                <a href="#toolsMenu" data-bs-toggle="collapse" class="{{ 'active' if active_page in ['azimuth', 'script'] else '' }}">
                    <i class="fa-solid fa-toolbox"></i> Tools
                    <i class="fa-solid fa-chevron-down ms-auto" style="width: auto; font-size: 0.8rem;"></i>
                </a>
                <ul class="collapse list-unstyled {{ 'show' if active_page in ['azimuth', 'script'] else '' }}" id="toolsMenu">
                    <li><a href="{{ url_for('azimuth') }}" class="{{ 'active' if active_page == 'azimuth' else '' }}" style="margin-left: 20px; font-size: 0.95rem;"><i class="fa-solid fa-compass"></i> Azimuth</a></li>
                    <li><a href="{{ url_for('script') }}" class="{{ 'active' if active_page == 'script' else '' }}" style="margin-left: 20px; font-size: 0.95rem;"><i class="fa-solid fa-code"></i> Script</a></li>
                </ul>
            </li>
            {% if current_user.role == 'admin' %}
            <li><a href="{{ url_for('import_data') }}" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Data Import</a></li>
            <li class="mt-4 mb-2 text-muted px-4 text-uppercase" style="font-size: 0.75rem; letter-spacing: 1px;">System</li>
            <li><a href="{{ url_for('manage_users') }}" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="{{ url_for('backup_restore') }}" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
            {% endif %}
            <li><a href="{{ url_for('profile') }}" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Profile</a></li>
            <li><a href="{{ url_for('logout') }}"><i class="fa-solid fa-right-from-bracket"></i> Logout</a></li>
        </ul>
    </div>

    <div class="main-content">
        <button class="btn btn-light shadow-sm d-md-none mb-3 border fw-bold" onclick="toggleSidebar()">
            <i class="fa-solid fa-bars me-1"></i> Menu
        </button>

        <div class="container-fluid p-0">
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
                <div class="modal-body p-4">
                    <div class="chart-container" style="position: relative; height:65vh; width:100%">
                        <canvas id="modalChart"></canvas>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        function toggleSidebar() {
            document.getElementById('sidebar').classList.toggle('active');
            document.getElementById('sidebar-overlay').classList.toggle('active');
        }

        let modalChartInstance = null;
        function showDetailModal(cellName, date, value, metricLabel, allDatasets, allLabels) {
            document.getElementById('modalTitle').innerText = 'Chi tiết ' + metricLabel;
            const ctx = document.getElementById('modalChart').getContext('2d');
            if (modalChartInstance) modalChartInstance.destroy();
            modalChartInstance = new Chart(ctx, {
                type: 'line',
                data: { labels: allLabels, datasets: allDatasets },
                options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false }, plugins: { legend: { display: true } }, spanGaps: true, elements: { line: { tension: 0.3 } } }
            });
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
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Đăng nhập | NetOps</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: linear-gradient(135deg, #f0f2f5 0%, #d9e2ec 100%); height: 100vh; display: flex; align-items: center; justify-content: center; font-family: 'Segoe UI', sans-serif; }
        .login-card { width: 100%; max-width: 400px; background: rgba(255, 255, 255, 0.9); backdrop-filter: blur(20px); padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); border: 1px solid rgba(255,255,255,0.5); }
        .btn-primary { background-color: #0078d4; border: none; padding: 10px; font-weight: 600; border-radius: 8px; transition: all 0.3s; width: 100%; }
        .btn-primary:hover { background-color: #0063b1; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0, 120, 212, 0.3); }
        .form-control { border-radius: 8px; padding: 12px; margin-bottom: 1rem; border: 1px solid #e0e0e0; }
        .form-control:focus { box-shadow: 0 0 0 3px rgba(0, 120, 212, 0.15); border-color: #0078d4; }
    </style>
</head>
<body>
    <div class="login-card">
        <h3 class="text-center mb-4 text-primary fw-bold">Welcome Back</h3>
        <p class="text-center text-muted mb-4">Sign in to access KPI Monitor</p>
        <form method="POST">
            <label class="form-label fw-bold text-secondary small">USERNAME</label>
            <input type="text" name="username" class="form-control" placeholder="Enter username" required>
            <label class="form-label fw-bold text-secondary small">PASSWORD</label>
            <input type="password" name="password" class="form-control" placeholder="Enter password" required>
            <button type="submit" class="btn btn-primary">Sign In</button>
        </form>
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
        <span class="badge bg-soft-primary text-primary px-3 py-2 rounded-pill border">{{ current_user.role | upper }}</span>
    </div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <h5 class="fw-bold text-secondary mb-4"><i class="fa-solid fa-chart-pie me-2"></i>Tổng quan Chất lượng Mạng 4G Toàn Hệ thống</h5>
            {% if dashboard_data and dashboard_data.labels %}
                <div class="row g-4">
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-primary mb-3">Tổng Traffic 4G (GB)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartTraffic"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-success mb-3">Trung bình User DL Thput (Mbps)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartThput"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-warning mb-3">Trung bình Tài nguyên PRB DL (%)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartPrb"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-info mb-3">Trung bình Chất lượng Vô tuyến (CQI 4G)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartCqi"></canvas></div></div></div></div>
                </div>
                <script id="dash-labels" type="application/json">{{ dashboard_data.labels | tojson if dashboard_data else [] }}</script>
                <script id="dash-traffic" type="application/json">{{ dashboard_data.traffic | tojson if dashboard_data else [] }}</script>
                <script id="dash-thput" type="application/json">{{ dashboard_data.thput | tojson if dashboard_data else [] }}</script>
                <script id="dash-prb" type="application/json">{{ dashboard_data.prb | tojson if dashboard_data else [] }}</script>
                <script id="dash-cqi" type="application/json">{{ dashboard_data.cqi | tojson if dashboard_data else [] }}</script>
                
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        if (typeof Chart === 'undefined') return;
                        const safeParse = (id) => { try { return JSON.parse(document.getElementById(id).textContent); } catch(e) { return []; } };
                        const labels = safeParse('dash-labels');
                        function createDashChart(id, label, color, bgColor, dataArr, titleStr) {
                            const ds = [{ label: label, data: dataArr, borderColor: color, backgroundColor: bgColor, fill: true, tension: 0.3, borderWidth: 2 }];
                            new Chart(document.getElementById(id).getContext('2d'), { type: 'line', data: { labels: labels, datasets: ds }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, interaction: { mode: 'nearest', axis: 'x', intersect: false }, onClick: (e, el) => { if (el.length > 0) showDetailModal(ds[0].label, labels[el[0].index], ds[0].data[el[0].index], titleStr, ds, labels); } } });
                        }
                        createDashChart('chartTraffic', 'Traffic (GB)', '#0078d4', 'rgba(0,120,212,0.1)', safeParse('dash-traffic'), 'Tổng Traffic 4G');
                        createDashChart('chartThput', 'Avg Thput (Mbps)', '#107c10', 'rgba(16,124,16,0.1)', safeParse('dash-thput'), 'Trung bình Thput');
                        createDashChart('chartPrb', 'Avg PRB (%)', '#ffaa44', 'rgba(255,170,68,0.1)', safeParse('dash-prb'), 'Trung bình PRB');
                        createDashChart('chartCqi', 'Avg CQI', '#00bcf2', 'rgba(0,188,242,0.1)', safeParse('dash-cqi'), 'Trung bình CQI');
                    });
                </script>
            {% else %}
                <div class="alert alert-info border-0 shadow-sm"><i class="fa-solid fa-circle-info me-2"></i>Chưa có dữ liệu KPI 4G để hiển thị biểu đồ.</div>
            {% endif %}

        {% elif active_page == 'azimuth' %}
            <div class="card border border-light shadow-sm">
                <div class="card-body p-1">
                    <div id="azimuthMap" style="height: 75vh; width: 100%; border-radius: 8px; z-index: 1;"></div>
                </div>
            </div>

            <!-- Panel công cụ -->
            <div id="azimuthFormContainer" class="shadow-lg" style="background: rgba(255, 255, 255, 0.95); padding: 15px; border-radius: 8px; width: 320px; max-width: 90vw; max-height: 65vh; overflow-y: auto; border: 1px solid #dee2e6;">
                <h6 class="fw-bold text-primary mb-2"><i class="fa-solid fa-compass me-2"></i>Tọa độ Điểm O (Gốc)</h6>
                <div class="mb-2 text-muted" style="font-size: 0.75rem;"><i class="fa-solid fa-info-circle me-1"></i><i>Mẹo: Click lên Bản đồ để chọn nhanh Điểm O</i></div>
                <div class="mb-2"><label class="form-label small fw-bold mb-1">Vĩ độ (Latitude)</label><input type="text" id="latO" class="form-control form-control-sm" placeholder="VD: 21.028511" required></div>
                <div class="mb-3"><label class="form-label small fw-bold mb-1">Kinh độ (Longitude)</label><input type="text" id="lngO" class="form-control form-control-sm" placeholder="VD: 105.804817" required></div>
                <button type="button" class="btn btn-outline-secondary btn-sm w-100 mb-3 fw-bold" onclick="getGPS()"><i class="fa-solid fa-location-crosshairs me-1"></i>Lấy GPS của tôi</button>
                <hr class="my-3">
                <h6 class="fw-bold text-success mb-2"><i class="fa-solid fa-pencil me-2"></i>Thêm Điểm Kết Nối</h6>
                <form id="azimuthForm">
                    <div class="mb-2"><label class="form-label small fw-bold mb-1">Tên điểm tới</label><input type="text" id="ptName" class="form-control form-control-sm" placeholder="VD: Trạm A" required></div>
                    <div class="mb-2"><label class="form-label small fw-bold mb-1">Góc Azimuth (Độ)</label><input type="number" id="ptAzimuth" class="form-control form-control-sm" min="0" max="360" step="any" placeholder="0 - 360" required></div>
                    <div class="mb-3"><label class="form-label small fw-bold mb-1">Khoảng cách (Mét)</label><input type="number" id="ptDistance" class="form-control form-control-sm" min="0" step="any" placeholder="Nhập số mét..." required></div>
                    <button type="submit" class="btn btn-primary btn-sm w-100 shadow-sm fw-bold mb-2"><i class="fa-solid fa-plus me-1"></i>Vẽ đường nối</button>
                    <button type="button" class="btn btn-danger btn-sm w-100 shadow-sm fw-bold" onclick="clearDrawnPoints()"><i class="fa-solid fa-trash-can me-1"></i>Xóa các đường đã vẽ</button>
                </form>
            </div>
            <script>
                var azMap, markerO;
                var drawnItems;
                var drawnPointsData = []; 
                document.addEventListener('DOMContentLoaded', function() {
                    if (typeof L === 'undefined') return;
                    drawnItems = L.layerGroup();
                    azMap = L.map('azimuthMap', {center: [16.0, 106.0], zoom: 5, zoomControl: true, fullscreenControl: true, fullscreenControlOptions: { position: 'topleft' }});
                    var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {maxZoom: 22, subdomains: ['mt0', 'mt1', 'mt2', 'mt3'], detectRetina: true, attribution: '© Google Maps'}).addTo(azMap);
                    var googleHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {maxZoom: 22, subdomains: ['mt0', 'mt1', 'mt2', 'mt3'], detectRetina: true, attribution: '© Google Maps'});
                    L.control.layers({"Bản đồ (Google)": googleStreets, "Vệ tinh (Google)": googleHybrid}, null, {position: 'topright'}).addTo(azMap);
                    drawnItems.addTo(azMap);
                    var formControl = L.control({position: 'topright'});
                    formControl.onAdd = function (map) {
                        var wrapper = L.DomUtil.create('div', 'leaflet-control');
                        var toggleBtn = L.DomUtil.create('button', 'btn btn-primary btn-sm shadow-lg mb-2 w-100 fw-bold', wrapper);
                        toggleBtn.innerHTML = '<i class="fa-solid fa-sliders me-1"></i>Công Cụ Vẽ';
                        toggleBtn.style.border = '2px solid white'; toggleBtn.style.borderRadius = '8px';
                        var formDiv = document.getElementById('azimuthFormContainer');
                        wrapper.appendChild(formDiv);
                        if (window.innerWidth <= 768) formDiv.style.display = 'none';
                        L.DomEvent.disableClickPropagation(wrapper); L.DomEvent.disableScrollPropagation(wrapper);
                        L.DomEvent.on(toggleBtn, 'click', function(e) { L.DomEvent.preventDefault(e); L.DomEvent.stopPropagation(e); formDiv.style.display = formDiv.style.display === 'none' ? 'block' : 'none'; });
                        return wrapper;
                    };
                    formControl.addTo(azMap);
                    azMap.on('click', function(e) { if (!markerO) { document.getElementById('latO').value = e.latlng.lat.toFixed(6); document.getElementById('lngO').value = e.latlng.lng.toFixed(6); drawOrigin(); } });
                    document.getElementById('azimuthForm').addEventListener('submit', function(e) { e.preventDefault(); drawAzimuth(); });
                });
                function flyToOrigin(lat, lng) { if (azMap) azMap.flyTo([lat, lng], 17, { animate: true, duration: 1.5 }); }
                function getGPS() {
                    if (navigator.geolocation) navigator.geolocation.getCurrentPosition(function(position) { document.getElementById('latO').value = position.coords.latitude.toFixed(6); document.getElementById('lngO').value = position.coords.longitude.toFixed(6); flyToOrigin(position.coords.latitude, position.coords.longitude); drawOrigin(); }, function(error) { alert("Lỗi không lấy được GPS: " + error.message); });
                    else alert("Trình duyệt của bạn không hỗ trợ Geolocation.");
                }
                function calculateDistanceAndBearing(lat1, lon1, lat2, lon2) {
                    const R = 6371e3; const f1 = lat1 * Math.PI/180; const f2 = lat2 * Math.PI/180; const df = (lat2-lat1) * Math.PI/180; const dl = (lon2-lon1) * Math.PI/180;
                    const a = Math.sin(df/2) * Math.sin(df/2) + Math.cos(f1) * Math.cos(f2) * Math.sin(dl/2) * Math.sin(dl/2); const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); const dist = R * c;
                    const y = Math.sin(dl) * Math.cos(f2); const x = Math.cos(f1)*Math.sin(f2) - Math.sin(f1)*Math.cos(f2)*Math.cos(dl);
                    let brng = Math.atan2(y, x) * 180 / Math.PI; return { distance: dist, bearing: (brng + 360) % 360 };
                }
                function updateAllLines() {
                    var latO = parseFloat(document.getElementById('latO').value); var lngO = parseFloat(document.getElementById('lngO').value); if(isNaN(latO) || isNaN(lngO)) return;
                    drawnPointsData.forEach(function(obj) { var posB = obj.marker.getLatLng(); var calc = calculateDistanceAndBearing(latO, lngO, posB.lat, posB.lng); obj.line.setLatLngs([[latO, lngO], posB]); var popupContent = "<div class='text-center'><b>" + obj.name + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + calc.bearing.toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + calc.distance.toFixed(2) + " m</b></div>"; if(obj.marker.getPopup()) obj.marker.getPopup().setContent(popupContent); });
                }
                function drawOrigin() {
                    if (typeof L === 'undefined') return;
                    var latO = document.getElementById('latO').value; var lngO = document.getElementById('lngO').value; if (!latO || !lngO) return;
                    if (markerO) { azMap.removeLayer(markerO); }
                    var iconO = L.divIcon({className: 'custom-div-icon', html: "<div style='background-color:#c0392b;width:18px;height:18px;border-radius:50%;border:3px solid white;box-shadow:0 0 8px rgba(0,0,0,0.8);'></div>", iconSize: [18, 18], iconAnchor: [9, 9]});
                    markerO = L.marker([latO, lngO], {icon: iconO, draggable: true}).bindTooltip("<b class='text-danger'>Điểm O</b>", {permanent: true, direction: 'left', className: 'bg-white border-danger rounded shadow-sm px-1 py-0'}).addTo(azMap);
                    markerO.on('drag', function(e) { var newPos = e.target.getLatLng(); document.getElementById('latO').value = newPos.lat.toFixed(6); document.getElementById('lngO').value = newPos.lng.toFixed(6); updateAllLines(); }); updateAllLines();
                }
                document.getElementById('latO').addEventListener('change', function() { drawOrigin(); var lat = document.getElementById('latO').value; var lng = document.getElementById('lngO').value; if(lat && lng) flyToOrigin(lat, lng); });
                document.getElementById('lngO').addEventListener('change', function() { drawOrigin(); var lat = document.getElementById('latO').value; var lng = document.getElementById('lngO').value; if(lat && lng) flyToOrigin(lat, lng); });
                function calculateDestinationPoint(lat1, lon1, brng, dist) {
                    const R = 6371e3; const d = parseFloat(dist); const brngRad = parseFloat(brng) * Math.PI / 180; const lat1Rad = parseFloat(lat1) * Math.PI / 180; const lon1Rad = parseFloat(lon1) * Math.PI / 180;
                    const lat2Rad = Math.asin(Math.sin(lat1Rad) * Math.cos(d/R) + Math.cos(lat1Rad) * Math.sin(d/R) * Math.cos(brngRad)); const lon2Rad = lon1Rad + Math.atan2(Math.sin(brngRad) * Math.sin(d/R) * Math.cos(lat1Rad), Math.cos(d/R) - Math.sin(lat1Rad) * Math.sin(lat2Rad)); return [lat2Rad * 180 / Math.PI, lon2Rad * 180 / Math.PI];
                }
                function drawAzimuth() {
                    if (typeof L === 'undefined') return;
                    var latO = document.getElementById('latO').value; var lngO = document.getElementById('lngO').value; var ptName = document.getElementById('ptName').value; var az = document.getElementById('ptAzimuth').value; var dist = document.getElementById('ptDistance').value;
                    if (!latO || !lngO || !ptName || !az || !dist) { alert("Vui lòng nhập đủ Điểm O, Tên điểm, Góc và Khoảng cách!"); return; }
                    drawOrigin(); var pointB = calculateDestinationPoint(latO, lngO, az, dist);
                    var iconB = L.divIcon({className: 'custom-div-icon', html: "<div style='background-color:#2980b9;width:14px;height:14px;border-radius:50%;border:2px solid white;box-shadow:0 0 6px rgba(0,0,0,0.6);'></div>", iconSize: [14, 14], iconAnchor: [7, 7]});
                    var popupContent = "<div class='text-center'><b>" + ptName + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + parseFloat(az).toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + parseFloat(dist).toFixed(2) + " m</b></div>";
                    var markerB = L.marker(pointB, {icon: iconB, draggable: true}).bindTooltip("<b>" + ptName + "</b>", {permanent: true, direction: 'right', className: 'text-primary border-primary rounded shadow-sm px-1 py-0'}).bindPopup(popupContent, {autoPan: false}).addTo(drawnItems);
                    markerB.on('dragstart', function(e) { this.openPopup(); });
                    var polyline = L.polyline([[latO, lngO], pointB], {color: '#000000', weight: 4, opacity: 1.0}).addTo(drawnItems);
                    var drawnObj = { marker: markerB, line: polyline, name: ptName }; drawnPointsData.push(drawnObj);
                    markerB.on('drag', function(e) { var newPos = e.target.getLatLng(); var curLatO = parseFloat(document.getElementById('latO').value); var curLngO = parseFloat(document.getElementById('lngO').value); var calc = calculateDistanceAndBearing(curLatO, curLngO, newPos.lat, newPos.lng); drawnObj.line.setLatLngs([[curLatO, curLngO], newPos]); var newPopup = "<div class='text-center'><b>" + ptName + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + calc.bearing.toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + calc.distance.toFixed(2) + " m</b></div>"; drawnObj.marker.getPopup().setContent(newPopup); });
                    var group = new L.featureGroup([markerO, drawnItems]); azMap.fitBounds(group.getBounds(), {padding: [50, 50]});
                    document.getElementById('ptName').value = ''; document.getElementById('ptAzimuth').value = ''; document.getElementById('ptDistance').value = ''; document.getElementById('ptName').focus();
                }
                function clearDrawnPoints() { if(drawnItems) drawnItems.clearLayers(); drawnPointsData = []; if(markerO && azMap) azMap.setView(markerO.getLatLng(), 15); }
            </script>
            
        {% elif active_page == 'optimize' %}
            <div class="alert alert-info border-0 shadow-sm mb-4">
                <h5 class="fw-bold text-primary mb-3"><i class="fa-solid fa-book-open-reader me-2"></i>Quy trình Tối ưu 5 Bước</h5>
                <ol class="mb-0 text-dark">
                    <li class="mb-1"><strong>Thu thập Vĩ mô:</strong> Tự động lọc Top Cell tệ từ báo cáo QoE/QoS Tuần.</li>
                    <li class="mb-1"><strong>Chẩn đoán Vi mô:</strong> Tự động ghép nối KPI Ngày để tìm nguyên nhân gốc rễ (Nghẽn, Nhiễu, Lỗi Phần cứng).</li>
                    <li class="mb-1"><strong>Giải pháp Tối ưu:</strong> Đề xuất hành động cho RNO, NOC, UCTT xử lý.</li>
                    <li class="mb-1"><strong>Giám sát Tức thời:</strong> Xem nhanh biểu đồ KPI sau khi thực hiện tác động.</li>
                    <li><strong>Đóng vòng Tối ưu:</strong> Theo dõi sự cải thiện điểm QoE/QoS ở tuần tiếp theo.</li>
                </ol>
            </div>
            
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/optimize" class="row g-3 align-items-center bg-white p-3 rounded-3 border shadow-sm">
                        <div class="col-md-8">
                            <label class="form-label fw-bold small text-muted">CHỌN TUẦN PHÂN TÍCH (BƯỚC 1)</label>
                            <select name="week_name" class="form-select border-0 shadow-sm bg-light">
                                {% for w in all_weeks %}
                                <option value="{{ w }}" {% if w == latest_week %}selected{% endif %}>{{ w }}</option>
                                {% endfor %}
                            </select>
                        </div>
                        <div class="col-md-4 align-self-end d-flex gap-2">
                            <button type="submit" name="action" value="filter" class="btn btn-danger w-100 shadow-sm"><i class="fa-solid fa-filter me-1"></i>Lọc</button>
                            <button type="submit" name="action" value="export" class="btn btn-success w-100 shadow-sm"><i class="fa-solid fa-file-excel me-1"></i>Export</button>
                        </div>
                    </form>
                </div>
            </div>
            
            <div class="d-flex justify-content-between align-items-center mb-3 mt-4">
                <h6 class="fw-bold text-danger mb-0"><i class="fa-solid fa-list-check me-2"></i>Danh sách Trạm Cần Xử lý ({{ latest_week or 'Chưa có dữ liệu' }})</h6>
            </div>
            
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;">
                 <table class="table table-hover table-bordered mb-0 align-middle" style="font-size: 0.85rem;">
                     <thead class="table-light text-center position-sticky top-0" style="z-index: 10;">
                         <tr>
                             <th rowspan="2" class="align-middle">Cell Name</th>
                             <th colspan="2">1. Báo cáo Tuần (Macro)</th>
                             <th colspan="4">2. KPI Ngày Gần Nhất (Micro)</th>
                             <th rowspan="2" class="align-middle">3. Chẩn đoán (Bệnh)</th>
                             <th rowspan="2" class="align-middle">4. Giải pháp (Action)</th>
                             <th rowspan="2" class="align-middle">5. Giám sát</th>
                         </tr>
                         <tr>
                             <th>Điểm QoE</th><th>Điểm QoS</th>
                             <th>PRB (%)</th><th>Thput (Mbps)</th><th>CQI (%)</th><th>Drop (%)</th>
                         </tr>
                     </thead>
                     <tbody>
                         {% for row in optimized_data %}
                         <tr>
                             <td class="fw-bold text-primary text-nowrap">{{ row.cell_name }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.qoe_score != '-' and row.qoe_score <= 2 }}">{{ row.qoe_score }}{% if row.qoe_percent != '-' %}<br><small class="text-muted">({{ row.qoe_percent }}%)</small>{% endif %}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.qos_score != '-' and row.qos_score <= 3 }}">{{ row.qos_score }}{% if row.qos_percent != '-' %}<br><small class="text-muted">({{ row.qos_percent }}%)</small>{% endif %}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.prb != '-' and row.prb > 20 }}">{{ row.prb }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.thput != '-' and row.thput < 10 }}">{{ row.thput }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.cqi != '-' and row.cqi < 93 }}">{{ row.cqi }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.drop != '-' and row.drop > 0.3 }}">{{ row.drop }}</td>
                             <td>
                                 <ul class="mb-0 ps-3 text-danger fw-bold" style="min-width: 150px;">
                                     {% for issue in row.issues %}<li>{{ issue }}</li>{% endfor %}
                                 </ul>
                             </td>
                             <td>
                                 <ul class="mb-0 ps-3 text-success" style="min-width: 180px;">
                                     {% for action in row.actions %}<li>{{ action }}</li>{% endfor %}
                                 </ul>
                             </td>
                             <td class="text-center p-2" style="min-width: 100px;">
                                 <a href="/kpi?tech=4g&cell_name={{ row.cell_name }}" class="btn btn-sm btn-outline-primary mb-1 w-100 py-0 shadow-sm" style="font-size: 0.75rem;"><i class="fa-solid fa-chart-line me-1"></i>Xem KPI</a>
                                 <a href="/qoe-qos?cell_name={{ row.cell_name }}" class="btn btn-sm btn-outline-warning w-100 py-0 text-dark shadow-sm" style="font-size: 0.75rem;"><i class="fa-solid fa-star-half-stroke me-1"></i>Xem QoE</a>
                             </td>
                         </tr>
                         {% else %}
                         <tr><td colspan="10" class="text-center py-5 text-muted"><i class="fa-solid fa-face-smile fa-3x mb-3 text-success d-block"></i>Tuyệt vời! Không phát hiện Cell nào vi phạm ngưỡng tệ trong tuần gần nhất.</td></tr>
                         {% endfor %}
                     </tbody>
                 </table>
            </div>
        
        {% elif active_page == 'gis' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="POST" action="/gis" enctype="multipart/form-data" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option></select></div>
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">SITE CODE</label><input type="text" name="site_code" class="form-control border-0 shadow-sm" placeholder="VD: THA001" value="{{ site_code_input }}"></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted">CELL NAME</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="VD: THA001_1" value="{{ cell_name_input }}"></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted text-warning"><i class="fa-solid fa-file-lines me-1"></i>LOG ITS (.TXT/.CSV)</label><input type="file" name="its_file" class="form-control border-0 shadow-sm" accept=".txt,.csv" multiple></div>
                        <div class="col-md-2 d-flex flex-column gap-2 mt-4 pt-1"><button type="submit" name="action" value="search" class="btn btn-primary btn-sm w-100 shadow-sm fw-bold"><i class="fa-solid fa-search me-1"></i>Tìm kiếm</button><button type="submit" name="action" value="show_log" class="btn btn-warning btn-sm w-100 shadow-sm fw-bold text-white"><i class="fa-solid fa-route me-1"></i>Xem Log</button></div>
                    </form>
                </div>
            </div>
            <div class="card border border-light shadow-sm">
                <div class="card-body p-1 position-relative">
                    <div id="gisMap" style="height: 65vh; width: 100%; border-radius: 8px; z-index: 1;"></div>
                </div>
            </div>
            
            <script id="gis-data-json" type="application/json">{{ gis_data | tojson | safe if gis_data else [] }}</script>
            <script id="its-data-json" type="application/json">{{ its_data | tojson | safe if its_data else [] }}</script>
            
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    if (typeof L === 'undefined') return;
                    
                    const safeParse = (id) => { try { return JSON.parse(document.getElementById(id).textContent); } catch(e) { return []; } };
                    var gisData = safeParse('gis-data-json');
                    var itsData = safeParse('its-data-json');
                    
                    var actionType = "{{ action_type }}";
                    var searchSite = "{{ site_code_input }}";
                    var searchCell = "{{ cell_name_input }}";
                    var isShowIts = ("{{ 'true' if show_its else 'false' }}" === "true");
                    var hasGisData = gisData.length > 0;
                    var hasItsData = isShowIts && itsData.length > 0;
                    
                    if(!document.getElementById('gisMap')) return;

                    var mapCenter = [19.807, 105.776];
                    var mapZoom = 9;

                    var map = L.map('gisMap', {
                        center: mapCenter,
                        zoom: mapZoom,
                        zoomControl: true,
                        fullscreenControl: true,
                        fullscreenControlOptions: { position: 'topleft' }
                    });

                    var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    }).addTo(map);
                    
                    var googleHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    });

                    L.control.layers({"Bản đồ (Google)": googleStreets, "Vệ tinh (Google)": googleHybrid}).addTo(map);

                    // Add Custom Settings Control
                    if (hasGisData || hasItsData) {
                        var settingsControl = L.control({position: 'topright'});
                        settingsControl.onAdd = function (map) {
                            var div = L.DomUtil.create('div', 'info settings-control shadow-sm');
                            div.style.backgroundColor = 'rgba(255, 255, 255, 0.95)';
                            div.style.padding = '12px';
                            div.style.borderRadius = '8px';
                            div.style.border = '1px solid #dee2e6';
                            
                            L.DomEvent.disableClickPropagation(div);

                            var html = '<div class="d-flex flex-column gap-2">';

                            if (hasGisData) {
                                html += '<div class="d-flex align-items-center justify-content-between">';
                                html += '<label class="small fw-bold text-muted mb-0 me-2" style="white-space: nowrap;"><i class="fa-solid fa-wifi text-info me-1"></i>Bán kính quạt:</label>';
                                html += '<input type="range" class="form-range" id="sectorRadiusSlider" min="50" max="2000" step="50" value="350" style="width: 100px;">';
                                html += '<span id="sectorRadiusVal" class="small text-muted ms-2 fw-bold" style="min-width: 45px; text-align: right;">350m</span>';
                                html += '</div>';
                            }

                            if (hasItsData) {
                                html += '<div class="d-flex align-items-center justify-content-between">';
                                html += '<label class="small fw-bold text-muted mb-0 me-2" style="white-space: nowrap;"><i class="fa-solid fa-circle text-primary me-1"></i>Cỡ hạt Log:</label>';
                                html += '<input type="range" class="form-range" id="pointSizeSlider" min="1" max="10" value="3" style="width: 100px;">';
                                html += '</div>';

                                html += '<div class="d-flex align-items-center mt-1 pt-2 border-top">';
                                html += '<div class="form-check form-switch mb-0 w-100 d-flex align-items-center ps-0">';
                                html += '<label class="form-check-label small fw-bold text-muted me-3" for="showLinesToggle" style="cursor: pointer;"><i class="fa-solid fa-link text-success me-1"></i>Nối vệt tia</label>';
                                html += '<input class="form-check-input mt-0 ms-auto float-none" type="checkbox" id="showLinesToggle" checked style="cursor: pointer;">';
                                html += '</div>';
                                html += '</div>';
                            }

                            html += '</div>';
                            div.innerHTML = html;
                            return div;
                        };
                        settingsControl.addTo(map);
                    }

                    // Bounds Management
                    var bounds = [];
                    if (isShowIts && itsData.length > 0) {
                        itsData.forEach(function(pt) { bounds.push([pt.lat, pt.lon]); });
                    }
                    if (actionType === 'show_log' && gisData.length > 0) {
                        gisData.forEach(function(cell) { bounds.push([cell.lat, cell.lon]); });
                    }

                    if (actionType === 'search' && (searchSite || searchCell) && gisData.length > 0) {
                        var targetCell = gisData[0];
                        for (var i = 0; i < gisData.length; i++) {
                            var sCode = (gisData[i].site_code || "").toLowerCase();
                            var cName = (gisData[i].cell_name || "").toLowerCase();
                            var sInput = searchSite.toLowerCase();
                            var cInput = searchCell.toLowerCase();
                            if ((sInput && sCode.includes(sInput)) || (cInput && cName.includes(cInput))) { targetCell = gisData[i]; break; }
                        }
                        if (targetCell.lat && targetCell.lon) { map.setView([targetCell.lat, targetCell.lon], 15); } 
                        else { map.setView(mapCenter, mapZoom); }
                    } else if (bounds.length > 0) {
                        map.fitBounds(bounds, {padding: [30, 30], maxZoom: 16});
                    } else {
                        map.setView(mapCenter, mapZoom);
                    }

                    var siteLayerGroup = L.layerGroup().addTo(map);
                    var sectorLayerGroup = L.layerGroup().addTo(map);
                    var itsLayerGroup = L.layerGroup().addTo(map);
                    var cellLookup = {};
                    var renderedSites = {};

                    var techColors = {'3g': '#0078d4', '4g': '#107c10', '5g': '#ffaa44'};

                    function getSectorMidPoint(lat, lon, azimuth, distanceMeters) {
                        var latFactor = 111320;
                        var lonFactor = 111320 * Math.cos(lat * Math.PI / 180);
                        var radMap = (azimuth || 0) * Math.PI / 180;
                        var dx = (distanceMeters * Math.sin(radMap)) / lonFactor;
                        var dy = (distanceMeters * Math.cos(radMap)) / latFactor;
                        return [lat + dy, lon + dx];
                    }

                    function getSectorPolygon(lat, lon, azimuth, beamwidth, radiusMeters) {
                        var center = [lat, lon];
                        var points = [center];
                        var startAngle = azimuth - beamwidth / 2;
                        var endAngle = azimuth + beamwidth / 2;
                        var latFactor = 111320;
                        var lonFactor = 111320 * Math.cos(lat * Math.PI / 180);
                        for (var i = startAngle; i <= endAngle; i += 5) {
                            var radMap = i * Math.PI / 180;
                            var dx = (radiusMeters * Math.sin(radMap)) / lonFactor; 
                            var dy = (radiusMeters * Math.cos(radMap)) / latFactor; 
                            points.push([lat + dy, lon + dx]);
                        }
                        points.push(center);
                        return points;
                    }

                    gisData.forEach(function(cell) {
                        if(!cell.lat || !cell.lon) return;
                        if (!renderedSites[cell.site_code]) {
                            L.circleMarker([cell.lat, cell.lon], {radius: 5, color: '#333', weight: 1.5, fillColor: '#ffffff', fillOpacity: 1}).bindPopup("<b>Site Code:</b> " + cell.site_code).addTo(siteLayerGroup);
                            renderedSites[cell.site_code] = true;
                        }
                    });

                    function cVal(v) {
                        if (v === null || v === undefined) return "";
                        var s = String(v).trim();
                        if (s.endsWith(".0")) s = s.slice(0, -2);
                        return s;
                    }

                    function buildLookupAndDrawSectors() {
                        sectorLayerGroup.clearLayers();
                        cellLookup = {};

                        var radiusSlider = document.getElementById('sectorRadiusSlider');
                        var sectorRadius = radiusSlider ? parseInt(radiusSlider.value) : 350;
                        var valDisplay = document.getElementById('sectorRadiusVal');
                        if (valDisplay) valDisplay.innerText = sectorRadius + 'm';

                        var targetCellCoord = null;
                        var targetPolygon = null;

                        gisData.forEach(function(cell) {
                            if(!cell.lat || !cell.lon) return;

                            var isMatch = false;
                            if (actionType === 'search' && (searchSite || searchCell)) {
                                var sCode = (cell.site_code || "").toLowerCase();
                                var cName = (cell.cell_name || "").toLowerCase();
                                var sInput = searchSite.toLowerCase();
                                var cInput = searchCell.toLowerCase();
                                if ((sInput && sCode.includes(sInput)) || (cInput && cName.includes(cInput))) { 
                                    isMatch = true;
                                    if (!targetCellCoord) targetCellCoord = [cell.lat, cell.lon];
                                }
                            }

                            var tech = cell.tech;
                            var info = cell.info;
                            var key = "";

                            if (tech === '4g') {
                                var en = cVal(info.enodeb_id); var lc = cVal(info.lcrid);
                                if (en && lc) key = en + "_" + lc;
                            } else if (tech === '3g') {
                                var ci = cVal(info.ci);
                                if (ci) key = ci;
                            } else if (tech === '5g') {
                                 var gn = cVal(info.gnodeb_id); var lc5 = cVal(info.lcrid);
                                 if (gn && lc5) key = gn + "_" + lc5;
                            }

                            if (key) {
                                cellLookup[key] = getSectorMidPoint(cell.lat, cell.lon, cell.azi, sectorRadius * 0.65);
                            }

                            var color = techColors[cell.tech] || '#dc3545';
                            var polyPoints = getSectorPolygon(cell.lat, cell.lon, cell.azi, 60, sectorRadius);
                            var polygon = L.polygon(polyPoints, {
                                color: isMatch ? '#ff0000' : color, 
                                weight: isMatch ? 3 : 1, 
                                fillColor: isMatch ? '#ff0000' : color, 
                                fillOpacity: isMatch ? 0.7 : 0.35
                            }).addTo(sectorLayerGroup);

                            if (isMatch && !targetPolygon) targetPolygon = polygon;

                            var infoHtml = "<div style='max-height: 250px; overflow-y: auto; overflow-x: hidden;'><table class='table table-sm table-bordered mb-0' style='font-size: 0.8rem;'>";
                            for (const [k, v] of Object.entries(cell.info)) {
                                if (v !== null && v !== '' && v !== 'None') {
                                    infoHtml += "<tr><th class='text-muted bg-light w-50'>" + k.toUpperCase() + "</th><td class='fw-bold'>" + v + "</td></tr>";
                                }
                            }
                            infoHtml += "</table></div>";

                            polygon.bindPopup(
                                "<div class='mb-2 pb-2 border-bottom'><b>Cell:</b> <span class='text-primary fs-6'>" + cell.cell_name + "</span><br>" +
                                "<b>Site:</b> " + cell.site_code + "<br>" +
                                "<b>Tọa độ:</b> " + cell.lat + ", " + cell.lon + "</div>" + infoHtml,
                                { minWidth: 300, maxWidth: 450 }
                            );
                        });
                        
                        if (actionType === 'search' && targetCellCoord) {
                            map.setView(targetCellCoord, 16);
                            if (targetPolygon) setTimeout(() => targetPolygon.openPopup(), 600);
                        }

                        drawITSData();
                    }

                    function getSignalColor(tech, level) {
                        var t = (tech || '').toUpperCase();
                        if (t.includes('4G') || t.includes('LTE')) {
                            if (level >= -75) return '#0000FF'; // Blue
                            if (level >= -85) return '#00FF00'; // Green
                            if (level >= -95) return '#FFFF00'; // Yellow
                            if (level >= -105) return '#FFA500'; // Orange
                            if (level >= -115) return '#FF0000'; // Red
                            return '#000000'; // Black
                        } else { 
                            if (level >= -65) return '#0000FF'; // Blue
                            if (level >= -75) return '#00FF00'; // Green
                            if (level >= -85) return '#FFFF00'; // Yellow
                            if (level >= -95) return '#FFA500'; // Orange
                            if (level >= -105) return '#FF0000'; // Red
                            return '#000000'; // Black
                        }
                    }

                    function drawITSData() {
                        if (!isShowIts || itsData.length === 0) return;
                        itsLayerGroup.clearLayers();

                        var pointSizeSlider = document.getElementById('pointSizeSlider');
                        var pointSize = pointSizeSlider ? parseInt(pointSizeSlider.value) : 3;
                        var showLinesToggle = document.getElementById('showLinesToggle');
                        var showLines = showLinesToggle ? showLinesToggle.checked : false;

                        itsData.forEach(function(pt) {
                            var ptColor = getSignalColor(pt.tech, pt.level);
                            var ptCoord = [pt.lat, pt.lon];
                            
                            L.circleMarker(ptCoord, {radius: pointSize, fillColor: ptColor, color: "#000", weight: 0.5, fillOpacity: 0.9})
                            .bindPopup("<div class='small'><b>Tech:</b> " + pt.tech + "<br><b>Level:</b> <span class='fw-bold' style='color:"+ptColor+"'>" + pt.level + " dBm</span><br><b>Qual:</b> " + (pt.qual||'-') + "<br><b>Node/CellID:</b> " + (pt.node||'-') + " / " + pt.cellid + "</div>")
                            .addTo(itsLayerGroup);

                            if (showLines) {
                                var ptTech = (pt.tech || '').toLowerCase();
                                var key = "";
                                if (ptTech.includes('4g') || ptTech.includes('lte')) {
                                    if (pt.node && pt.cellid) key = pt.node + "_" + pt.cellid;
                                } else if (ptTech.includes('3g') || ptTech.includes('wcdma') || ptTech.includes('hspa') || ptTech.includes('umts')) {
                                    if (pt.cellid) key = pt.cellid;
                                } else if (ptTech.includes('5g') || ptTech.includes('nr')) {
                                    if (pt.node && pt.cellid) key = pt.node + "_" + pt.cellid;
                                }

                                var targetCellCoord = cellLookup[key];
                                if (targetCellCoord) {
                                    L.polyline([ptCoord, targetCellCoord], {color: ptColor, weight: 1.5, opacity: 0.6, dashArray: '3, 4'}).addTo(itsLayerGroup);
                                }
                            }
                        });
                    }

                    buildLookupAndDrawSectors();

                    var radSliderEl = document.getElementById('sectorRadiusSlider');
                    if (radSliderEl) radSliderEl.addEventListener('input', buildLookupAndDrawSectors);

                    var sliderEl = document.getElementById('pointSizeSlider');
                    if (sliderEl) sliderEl.addEventListener('input', drawITSData);

                    var toggleEl = document.getElementById('showLinesToggle');
                    if (toggleEl) toggleEl.addEventListener('change', drawITSData);

                    // Add Legend
                    if (isShowIts && itsData.length > 0) {
                        var legend = L.control({position: 'bottomright'});
                        legend.onAdd = function (map) {
                            var div = L.DomUtil.create('div', 'info legend shadow-sm');
                            div.style.backgroundColor = 'rgba(255, 255, 255, 0.95)';
                            div.style.padding = '10px 15px';
                            div.style.borderRadius = '8px';
                            div.style.fontSize = '0.85rem';
                            div.style.lineHeight = '1.8';
                            div.style.border = '1px solid #dee2e6';
                            
                            var html = '';
                            var has4G = itsData.some(pt => (pt.tech || '').toUpperCase().includes('4G') || (pt.tech || '').toUpperCase().includes('LTE'));
                            var has3G = itsData.some(pt => !(pt.tech || '').toUpperCase().includes('4G') && !(pt.tech || '').toUpperCase().includes('LTE'));

                            if (has4G) {
                                html += '<strong class="text-primary fs-6 d-block mb-2"><i class="fa-solid fa-signal me-1"></i> Chú giải 4G RSRP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -75)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-95 đến -85)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-115 đến -105)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -115)</div>';
                            }
                            if (has3G) {
                                if (has4G) html += '<hr class="my-2">';
                                html += '<strong class="text-success fs-6 d-block mb-2"><i class="fa-solid fa-signal me-1"></i> Chú giải 3G RSCP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -65)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-75 đến -65)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-95 đến -85)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -105)</div>';
                            }
                            div.innerHTML = html;
                            return div;
                        };
                        legend.addTo(map);
                    }
                });
            </script>

        {% elif active_page == 'kpi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/kpi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option></select></div>
                        <div class="col-md-4"><label class="form-label fw-bold small text-muted">TÌM THEO POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted">NHẬP CELL/SITE</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="Site code, Cell list..." value="{{ cell_name_input }}"></div>
                        <div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm">Visualize</button></div>
                    </form>
                </div>
            </div>
            {% if charts %}
                {% for chart_id, chart_config in charts.items() %}
                <div class="card mb-4 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ chart_config.title }}</h6><div class="chart-container" style="position: relative; height:45vh; width:100%"><canvas id="{{ chart_id }}" data-chart-data="{{ chart_config | tojson | forceescape }}"></canvas></div></div></div>
                {% endfor %}
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        if (typeof Chart === 'undefined') return;
                        document.querySelectorAll('canvas[data-chart-data]').forEach(canvas => {
                            try {
                                const cd = JSON.parse(canvas.getAttribute('data-chart-data'));
                                new Chart(canvas.getContext('2d'), {
                                    type: 'line', data: cd,
                                    options: { responsive: true, maintainAspectRatio: false, spanGaps: true, elements: { line: { tension: 0.3 } }, interaction: { mode: 'nearest', intersect: false, axis: 'x' }, onClick: (e, el) => { if (el.length > 0) { const i = el[0].index; const di = el[0].datasetIndex; showDetailModal(cd.datasets[di].label, cd.labels[i], cd.datasets[di].data[i], cd.title || '', cd.datasets, cd.labels); } }, plugins: { legend: { position: 'bottom' }, tooltip: { mode: 'index', intersect: false } } }
                                });
                            } catch(e) { console.error(e); }
                        });
                    });
                </script>
            {% elif cell_name_input or selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm"><i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu phù hợp.</div>
            {% else %}
                <div class="text-center text-muted py-5 opacity-50"><i class="fa-solid fa-chart-line fa-4x mb-3"></i><p class="fs-5">Vui lòng chọn tiêu chí để xem báo cáo KPI.</p></div>
            {% endif %}

        {% elif active_page == 'qoe_qos' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/qoe-qos" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-6"><label class="form-label fw-bold small text-muted">NHẬP CELL NAME 4G</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="VD: THA001_1" value="{{ cell_name_input }}" required></div>
                        <div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm"><i class="fa-solid fa-search me-2"></i>Tra cứu</button></div>
                        {% if has_data %}<div class="col-md-3 align-self-end"><a href="/kpi?tech=4g&cell_name={{ cell_name_input }}" class="btn btn-success w-100 shadow-sm"><i class="fa-solid fa-link me-2"></i>Link tới KPI Cell</a></div>{% endif %}
                    </form>
                </div>
            </div>
            {% if charts %}
                <div class="row">
                    {% for chart_id, c in charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ c.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ chart_id }}" data-chart-data="{{ c | tojson | forceescape }}"></canvas></div></div></div></div>
                    {% endfor %}
                </div>
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        if (typeof Chart === 'undefined') return;
                        document.querySelectorAll('canvas[data-chart-data]').forEach(canvas => {
                            try {
                                const cd = JSON.parse(canvas.getAttribute('data-chart-data'));
                                new Chart(canvas.getContext('2d'), {
                                    type: 'line', data: cd,
                                    options: { responsive: true, maintainAspectRatio: false, spanGaps: true, elements: { line: { tension: 0.3 } }, interaction: { mode: 'nearest', intersect: false, axis: 'x' }, onClick: (e, el) => { if (el.length > 0) showDetailModal(cd.datasets[el[0].datasetIndex].label, cd.labels[el[0].index], cd.datasets[el[0].datasetIndex].data[el[0].index], cd.title || '', cd.datasets, cd.labels); } }
                                });
                            } catch(e) { console.error(e); }
                        });
                    });
                </script>
                {% if qoe_details %}
                <div class="card mt-2 shadow-sm border-0 mb-4"><div class="card-header bg-white fw-bold text-primary">Dữ liệu gốc QoE Hàng tuần</div><div class="card-body p-0 table-responsive"><table class="table table-bordered table-striped table-hover mb-0 text-nowrap" style="font-size: 0.8rem;"><thead class="table-light"><tr><th>Tuần</th>{% for k in qoe_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qoe_details %}<tr><td class="fw-bold text-primary">{{ row.week }}</td>{% for k in qoe_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
                {% if qos_details %}
                <div class="card shadow-sm border-0 mb-4"><div class="card-header bg-white fw-bold text-success">Dữ liệu gốc QoS Hàng tuần</div><div class="card-body p-0 table-responsive"><table class="table table-bordered table-striped table-hover mb-0 text-nowrap" style="font-size: 0.8rem;"><thead class="table-light"><tr><th>Tuần</th>{% for k in qos_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qos_details %}<tr><td class="fw-bold text-success">{{ row.week }}</td>{% for k in qos_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
            {% elif cell_name_input %}
                <div class="alert alert-warning border-0 shadow-sm"><i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu QoE/QoS cho Cell: <strong>{{ cell_name_input }}</strong>.</div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-star-half-stroke fa-3x mb-3 opacity-50"></i><p>Nhập mã Cell 4G để xem biểu đồ xu hướng QoE, QoS.</p></div>
            {% endif %}

        {% elif active_page == 'poi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/poi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-8"><label class="form-label fw-bold small text-muted">CHỌN POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                        <div class="col-md-4 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm">Xem Báo Cáo</button></div>
                    </form>
                </div>
            </div>
            {% if poi_charts %}
                <div class="row">
                    {% for chart_id, c in poi_charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ c.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ chart_id }}" data-chart-data="{{ c | tojson | forceescape }}"></canvas></div></div></div></div>
                    {% endfor %}
                </div>
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        if (typeof Chart === 'undefined') return;
                        document.querySelectorAll('canvas[data-chart-data]').forEach(canvas => {
                            try {
                                const cd = JSON.parse(canvas.getAttribute('data-chart-data'));
                                new Chart(canvas.getContext('2d'), {
                                    type: 'line', data: cd,
                                    options: { responsive: true, maintainAspectRatio: false, spanGaps: true, elements: { line: { tension: 0.3 } }, interaction: { mode: 'nearest', intersect: false, axis: 'x' }, onClick: (e, el) => { if (el.length > 0) showDetailModal(cd.datasets[el[0].datasetIndex].label, cd.labels[el[0].index], cd.datasets[el[0].datasetIndex].data[el[0].index], cd.title || '', cd.datasets, cd.labels); } }
                                });
                            } catch(e) { console.error(e); }
                        });
                    });
                </script>
            {% elif selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm">Không có dữ liệu cho POI: <strong>{{ selected_poi }}</strong></div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-map-location-dot fa-3x mb-3"></i><p>Chọn địa điểm POI để xem báo cáo.</p></div>
            {% endif %}

        {% elif active_page == 'worst_cell' %}
            <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/worst-cell" class="row g-3 align-items-center bg-light p-3 rounded-3 border"><div class="col-auto"><label class="col-form-label fw-bold text-muted">THỜI GIAN</label></div><div class="col-auto"><select name="duration" class="form-select border-0 shadow-sm"><option value="1" {% if duration == 1 %}selected{% endif %}>1 ngày mới nhất</option><option value="3" {% if duration == 3 %}selected{% endif %}>3 ngày liên tiếp</option><option value="7" {% if duration == 7 %}selected{% endif %}>7 ngày liên tiếp</option><option value="15" {% if duration == 15 %}selected{% endif %}>15 ngày liên tiếp</option><option value="30" {% if duration == 30 %}selected{% endif %}>30 ngày liên tiếp</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-danger shadow-sm">Lọc Worst Cell</button></div><div class="col-auto"><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel me-2"></i>Export Excel</button></div></form></div></div>
            {% if dates %}<div class="alert alert-info border-0 shadow-sm mb-4"><i class="fa-solid fa-calendar-days me-2"></i><strong>Xét duyệt:</strong> {% for d in dates %}<span class="badge bg-white text-info border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 70vh;">
                <table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light position-sticky top-0" style="z-index: 10;"><tr><th>Cell Name</th><th class="text-center">Avg Thput</th><th class="text-center">Avg PRB</th><th class="text-center">Avg CQI</th><th class="text-center">Avg Drop Rate</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in worst_cells %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_thput < 7000 }}">{{ r.avg_thput }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_res_blk > 20 }}">{{ r.avg_res_blk }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_cqi < 93 }}">{{ r.avg_cqi }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_drop > 0.3 }}">{{ r.avg_drop }}</td><td class="text-center"><a href="/kpi?tech=4g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted">Nhấn "Lọc Worst Cell" để xem dữ liệu</td></tr>{% endfor %}</tbody></table>
            </div>

        {% elif active_page == 'traffic_down' %}
             <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/traffic-down" class="row g-3 align-items-center bg-light p-3 rounded-3 border"><div class="col-auto"><label class="col-form-label fw-bold text-muted">CÔNG NGHỆ:</label></div><div class="col-auto"><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if tech == '5g' %}selected{% endif %}>5G</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm">Thực hiện</button><button type="submit" name="action" value="export_zero" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> Zero</button><button type="submit" name="action" value="export_degraded" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> Degraded</button><button type="submit" name="action" value="export_poi_degraded" class="btn btn-warning shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> POI Degraded</button></div><div class="col-auto ms-auto"><span class="badge bg-info text-dark">Ngày phân tích: {{ analysis_date }}</span></div></form></div></div>
            <div class="row g-4">
                <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-danger text-white fw-bold">Cell Không Lưu Lượng (< 0.1 GB)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Avg (7 Days)</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in zero_traffic %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.avg_last_7 }}</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">Cell Suy Giảm (> 30%)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Last Week</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                 <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">POI Suy Giảm (> 30%)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>POI Name</th><th class="text-end">Today</th><th class="text-end">Last Week</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded_pois %}<tr><td class="fw-bold">{{ r.poi_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/poi?poi_name={{ r.poi_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
            </div>

        {% elif active_page == 'conges_3g' %}
            <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/conges-3g" class="d-flex align-items-center"><div class="alert alert-info border-0 shadow-sm bg-soft-primary text-primary mb-0 flex-grow-1"><strong>Điều kiện:</strong> (CS_CONG > 2% & CS_ATT > 100) OR (PS_CONG > 2% & PS_ATT > 500) (3 ngày liên tiếp)</div><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm ms-3">Thực hiện</button><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel me-2"></i>Export</button></form></div></div>
            {% if dates %}<div class="mb-3 text-muted small"><i class="fa-solid fa-calendar me-2"></i>Xét duyệt: {% for d in dates %}<span class="badge bg-light text-dark border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border"><table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light"><tr><th>Cell Name</th><th>Avg CS Traffic</th><th>Avg CS Conges (%)</th><th>Avg PS Traffic</th><th>Avg PS Conges (%)</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in conges_data %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td>{{ r.avg_cs_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_cs_conges > 2 }}">{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_ps_conges > 2 }}">{{ r.avg_ps_conges }}</td><td class="text-center"><a href="/kpi?tech=3g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white shadow-sm">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted opacity-50">Nhấn nút "Thực hiện" để xem kết quả</td></tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'rf' %}
             <div class="d-flex flex-wrap justify-content-between align-items-center mb-4 bg-white p-3 rounded shadow-sm border gap-3">
                 <div class="btn-group shadow-sm">
                     <a href="/rf?tech=3g" class="btn {{ 'btn-primary' if current_tech == '3g' else 'btn-outline-primary' }}">3G</a>
                     <a href="/rf?tech=4g" class="btn {{ 'btn-primary' if current_tech == '4g' else 'btn-outline-primary' }}">4G</a>
                     <a href="/rf?tech=5g" class="btn {{ 'btn-primary' if current_tech == '5g' else 'btn-outline-primary' }}">5G</a>
                 </div>
                 
                 <form method="GET" action="/rf" class="d-flex flex-grow-1 mx-lg-4">
                     <input type="hidden" name="tech" value="{{ current_tech }}">
                     <div class="input-group shadow-sm">
                         <span class="input-group-text bg-light border-end-0"><i class="fa-solid fa-search text-muted"></i></span>
                         <input type="text" name="cell_search" class="form-control border-start-0 ps-0" placeholder="Nhập Cell Code hoặc Site Code để tìm nhanh..." value="{{ search_query }}">
                         <button type="submit" class="btn btn-primary px-4 fw-bold">Tìm kiếm</button>
                     </div>
                 </form>

                 <div class="d-flex gap-2">
                     <form method="GET" action="/rf" class="m-0">
                         <input type="hidden" name="tech" value="{{ current_tech }}">
                         <input type="hidden" name="cell_search" value="{{ search_query }}">
                         <button type="submit" name="action" value="export" class="btn btn-success shadow-sm text-white fw-bold"><i class="fa-solid fa-file-excel me-2"></i>Export</button>
                     </form>
                     {% if current_user.role == 'admin' %}
                     <a href="/rf/add?tech={{ current_tech }}" class="btn btn-warning shadow-sm fw-bold"><i class="fa-solid fa-plus me-1"></i>New</a>
                     {% endif %}
                 </div>
             </div>
             
             <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;">
                 <table class="table table-hover mb-0" style="font-size: 0.85rem; white-space: nowrap;">
                     <thead class="table-light position-sticky top-0" style="z-index: 10;">
                         <tr>
                             <th class="text-center border-bottom bg-light" style="position: sticky; left: 0; z-index: 20;">Action</th>
                             {% for col in rf_columns %}<th>{{ col | replace('_', ' ') | upper }}</th>{% endfor %}
                         </tr>
                     </thead>
                     <tbody>
                         {% for row in rf_data %}
                         <tr>
                             <td class="text-center bg-white border-end shadow-sm" style="position: sticky; left: 0; z-index: 5;">
                                 <a href="/rf/detail/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-primary py-0"><i class="fa-solid fa-eye"></i></a>
                                 {% if current_user.role == 'admin' %}
                                 <a href="/rf/edit/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-warning py-0"><i class="fa-solid fa-pen"></i></a>
                                 <a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-danger py-0" onclick="return confirm('Xóa?')"><i class="fa-solid fa-trash"></i></a>
                                 {% endif %}
                             </td>
                             {% for col in rf_columns %}<td>{{ row[col] }}</td>{% endfor %}
                         </tr>
                         {% else %}
                         <tr><td colspan="100%" class="text-center py-4 text-muted"><i class="fa-solid fa-magnifying-glass fa-2x mb-2 d-block opacity-50"></i>Không tìm thấy trạm nào.</td></tr>
                         {% endfor %}
                     </tbody>
                 </table>
             </div>
        
        {% elif active_page == 'import' %}
             <div class="row">
                 <div class="col-md-8">
                     <div class="tab-content bg-white p-4 rounded-3 shadow-sm border">
                         <h5 class="mb-3 text-primary"><i class="fa-solid fa-cloud-arrow-up me-2"></i>Data Import</h5>
                         <ul class="nav nav-tabs mb-4" id="importTabs" role="tablist">
                             <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabRF" type="button">Import RF</button></li>
                             <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabPOI" type="button">Import POI</button></li>
                             <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabKPI" type="button">Import KPI</button></li>
                             <li class="nav-item"><button class="nav-link text-primary fw-bold" data-bs-toggle="tab" data-bs-target="#tabQoE" type="button">Import QoE/QoS</button></li>
                             <li class="nav-item"><button class="nav-link text-danger fw-bold" data-bs-toggle="tab" data-bs-target="#tabReset" type="button">Reset Data</button></li>
                         </ul>
                         <div class="tab-content">
                             <div class="tab-pane fade show active" id="tabRF">
                                 <div class="row">
                                     <div class="col-md-7 border-end">
                                         <form action="/import" method="POST" enctype="multipart/form-data">
                                             <div class="mb-3">
                                                 <label class="form-label fw-bold">Chọn Loại Dữ Liệu RF</label>
                                                 <select name="type" class="form-select">
                                                     <option value="cell3g">1. CELL 3G (Thông số trạm, Tọa độ)</option>
                                                     <option value="config3g">2. CONFIG 3G (Thông số mạng)</option>
                                                     <option value="4g">RF 4G</option>
                                                     <option value="5g">RF 5G</option>
                                                 </select>
                                             </div>
                                             <div class="mb-3">
                                                 <label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label>
                                                 <input type="file" name="file" class="form-control" multiple required>
                                             </div>
                                             <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload File</button>
                                         </form>
                                     </div>
                                     <div class="col-md-5 d-flex flex-column justify-content-center px-4">
                                         <div class="alert alert-info py-2 small mb-3 border-0 shadow-sm"><i class="fa-solid fa-circle-info me-1"></i> Đối với 3G, upload 2 file (CELL và CONFIG) rồi nhấn đồng bộ.</div>
                                         <form action="/sync-rf3g" method="POST">
                                             <button type="submit" class="btn btn-success w-100 fw-bold shadow-sm py-3" onclick="return confirm('Bạn có chắc chắn muốn gộp dữ liệu Cell 3G và Config 3G vào bảng RF 3G chính không?');"><i class="fa-solid fa-object-group me-2"></i>Ghép nối dữ liệu 3G</button>
                                         </form>
                                     </div>
                                 </div>
                             </div>
                             <div class="tab-pane fade" id="tabPOI">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu POI</label><select name="type" class="form-select"><option value="poi4g">POI 4G</option><option value="poi5g">POI 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload POI Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabKPI">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu KPI</label><select name="type" class="form-select"><option value="kpi3g">KPI 3G</option><option value="kpi4g">KPI 4G</option><option value="kpi5g">KPI 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload KPI Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabQoE">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold text-primary">Chọn Loại Dữ Liệu QoE/QoS</label><select name="type" class="form-select border-primary"><option value="qoe4g">QoE 4G (Hàng Tuần)</option><option value="qos4g">QoS 4G (Hàng Tuần)</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Tên Tuần</label><input type="text" name="week_name" class="form-control" value="{{ default_week_name }}" required></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabReset">
                                 <div class="alert alert-warning border-0 shadow-sm mb-4"><i class="fa-solid fa-triangle-exclamation me-2"></i><strong>Cảnh báo:</strong> Đã cập nhật Schema. Vui lòng bấm Reset trước khi import dữ liệu RF.</div>
                                 <form action="/reset-data" method="POST" onsubmit="return confirm('Chắc chắn reset toàn bộ dữ liệu RF?');" class="mb-3"><input type="hidden" name="target" value="rf"><button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu RF</button></form>
                                 <form action="/reset-data" method="POST" onsubmit="return confirm('Chắc chắn reset toàn bộ POI?');"><input type="hidden" name="target" value="poi"><button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu POI</button></form>
                             </div>
                         </div>
                     </div>
                 </div>
                 <div class="col-md-4">
                     <div class="card h-100 border-0 shadow-sm"><div class="card-header bg-white fw-bold text-success border-bottom">Data History</div><div class="card-body p-0 overflow-auto" style="max-height: 400px;"><table class="table table-sm table-striped mb-0 text-center"><thead class="table-light sticky-top"><tr><th>3G</th><th>4G</th><th>5G</th></tr></thead><tbody>{% for r3, r4, r5 in kpi_rows %}<tr><td>{{ r3 or '-' }}</td><td>{{ r4 or '-' }}</td><td>{{ r5 or '-' }}</td></tr>{% endfor %}</tbody></table></div></div>
                 </div>
             </div>
        
        {% elif active_page == 'script' %}
             <div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Generate Script</div><div class="card-body">
                <ul class="nav nav-tabs mb-3" id="scriptTabs" role="tablist">
                    <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab3g900" type="button">3G 900</button></li>
                    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab4g" type="button">4G</button></li>
                    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab3g2100" type="button">3G 2100</button></li>
                </ul>
                <div class="tab-content">
                    <div class="tab-pane fade show active" id="tab3g900">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="3g900"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g900"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="70"></td><td><input type="number" name="hsn[]" class="form-control" value="2"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g900')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                    </div>
                     <div class="tab-pane fade" id="tab4g">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="4g"><div class="table-responsive"><table class="table table-bordered" id="rruTable_4g"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="60"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="4"></td><td><input type="number" name="txnum[]" class="form-control" value="4"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('4g')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                     </div>
                     <div class="tab-pane fade" id="tab3g2100">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="3g2100"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g2100"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="80"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g2100')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                     </div>
                </div>
                {% if script_result %}
                <div class="mt-4"><h5 class="fw-bold text-primary">Result:</h5><textarea class="form-control font-monospace bg-light border-0" rows="12" readonly>{{ script_result }}</textarea></div>
                {% endif %}
             </div></div>

        {% endif %}
    </div>
</div>
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
# 5. ROUTES IMPLEMENTATION
# ==============================================================================

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
                    if file.filename.endswith('.csv'):
                        df_raw = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip', header=None, dtype=str)
                    else:
                        df_raw = pd.read_excel(file, header=None, dtype=str)
                    
                    header_idx = 0
                    max_matches = 0
                    kw = ['cell', 'site', 'trạm', 'uarfcn', 'hệ thống', 'quản lý', 'thiết bị', 'lat', 'long', 'stt', 'node', 'bsc', 'rnc', 'azimuth', 'tilt', 'power', 'gain']
                    
                    if len(df_raw) > 0:
                        for i in range(min(20, len(df_raw))):
                            row_vals = [str(v).lower() for v in df_raw.iloc[i].values if pd.notna(v)]
                            matches = sum(1 for k in kw if any(k in val for val in row_vals))
                            if matches > max_matches:
                                max_matches = matches
                                header_idx = i
                    
                    if max_matches > 0:
                        raw_cols = [str(c).strip() for c in df_raw.iloc[header_idx].values]
                        seen = {}
                        for j, c in enumerate(raw_cols):
                            if c in seen:
                                seen[c] += 1
                                raw_cols[j] = f"{c}_{seen[c]}"
                            else:
                                seen[c] = 0
                        df_raw.columns = raw_cols
                        df_raw = df_raw.iloc[header_idx + 1:].reset_index(drop=True)
                    else:
                        df_raw.columns = [str(c) for c in df_raw.iloc[0].values]
                        df_raw = df_raw.iloc[1:].reset_index(drop=True)

                    df_raw = df_raw.dropna(how='all')
                    original_columns = list(df_raw.columns)
                    df_raw.columns = [clean_header(c) for c in df_raw.columns]
                    header_mapping = dict(zip(df_raw.columns, original_columns))
                    
                    records = []
                    inserted_count = 0
                    BATCH_SIZE = 500
                    
                    for index, row in df_raw.iterrows():
                        clean_row, extra = {}, {}
                        for k, v in row.items():
                            if pd.isna(v): continue
                            val_str = str(v).strip()
                            if val_str in ['', '-', 'nan', 'None', 'N/A', 'null', 'NULL']: continue
                            
                            if k in valid_cols:
                                col_type = str(Model.__table__.columns[k].type)
                                if 'FLOAT' in col_type or 'INTEGER' in col_type:
                                    try:
                                        v_num = float(val_str)
                                        if 'INTEGER' in col_type: v_num = int(v_num)
                                        clean_row[k] = v_num
                                    except:
                                        clean_row[k] = val_str
                                else:
                                    clean_row[k] = val_str
                            else: 
                                extra[header_mapping.get(k, k)] = val_str
                        
                        c_code = clean_row.get('cell_code') or clean_row.get('cell_name') or clean_row.get('ten_tren_he_thong') or clean_row.get('ma_node') or clean_row.get('site_code')
                        
                        if not c_code and extra:
                            for ex_k, ex_v in extra.items():
                                w_lower = str(ex_k).lower()
                                if any(word in w_lower for word in ['cell', 'site', 'trạm', 'node', 'hệ thống']):
                                    c_code = ex_v
                                    break
                        
                        if c_code and str(c_code).strip() not in ['', 'nan', 'None']:
                            c_code_clean = str(c_code).strip()
                            clean_row['cell_code'] = c_code_clean
                            
                            if hasattr(Model, 'extra_data') and extra: 
                                clean_row['extra_data'] = json.dumps(extra, ensure_ascii=False)
                                
                            records.append(clean_row)
                            
                        if len(records) >= BATCH_SIZE:
                            db.session.bulk_insert_mappings(Model, records)
                            db.session.commit()
                            inserted_count += len(records)
                            records = [] 
                            gc.collect()
                    
                    if records:
                        db.session.bulk_insert_mappings(Model, records)
                        db.session.commit()
                        inserted_count += len(records)
                        
                    if inserted_count > 0:
                        flash(f'Đã import thành công {inserted_count} dòng vào {itype.upper()}.', 'success')
                    else:
                        found_cols = ", ".join([str(c) for c in original_columns[:10]])
                        flash(f'Lỗi file {file.filename}: Không tìm thấy dữ liệu hợp lệ. Các cột tìm thấy: {found_cols}', 'warning')
                        
                except Exception as e: 
                    err_msg = str(e)
                    db.session.rollback()
                    if 'Unknown column' in err_msg:
                        flash('CẤU TRÚC DB BỊ LỖI: Bạn cần Reset Database cấu hình trước khi import!', 'danger')
                    else:
                        flash(f'Lỗi file {file.filename}: {err_msg}', 'danger')
        
        elif itype in ['qoe4g', 'qos4g']:
            week_name = request.form.get('week_name', 'Tuần')
            TargetModel = QoE4G if itype == 'qoe4g' else QoS4G
            for file in files:
                try:
                    df = pd.read_excel(file, header=None, dtype=str) if file.filename.endswith('.xlsx') else pd.read_csv(file, header=None, dtype=str)
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
                        inserted_count = 0
                        BATCH_SIZE = 500
                        
                        for i in range(header_row_idx + 1, len(df)):
                            row_data = df.iloc[i]
                            c_name = str(row_data[cell_col_idx]).strip()
                            if not c_name or str(c_name).lower() in ['nan', 'none', 'null', ''] or len(str(c_name)) < 5 or str(c_name).isdigit(): continue
                            
                            try: val1 = float(row_data[cell_col_idx + 2])
                            except: val1 = 0.0
                            try: val2 = float(row_data[cell_col_idx + 3])
                            except: val2 = 0.0
                            
                            if math.isnan(val1): val1 = 0.0
                            if math.isnan(val2): val2 = 0.0
                                
                            percent, score = max(val1, val2), min(val1, val2)
                            details_dict = {headers[j]: str(row_data[j]).strip() for j in range(len(headers)) if pd.notna(row_data[j]) and str(row_data[j]).strip() not in ['nan', 'None', '']}
                            details_json = json.dumps(details_dict, ensure_ascii=False)
                            
                            records.append({'cell_name': c_name, 'week_name': week_name, 'qoe_score' if itype == 'qoe4g' else 'qos_score': score, 'qoe_percent' if itype == 'qoe4g' else 'qos_percent': percent, 'details': details_json})
                            
                            if len(records) >= BATCH_SIZE:
                                db.session.bulk_insert_mappings(TargetModel, records)
                                db.session.commit()
                                inserted_count += len(records)
                                records = []
                                gc.collect()
                                
                        if records:
                            db.session.bulk_insert_mappings(TargetModel, records)
                            db.session.commit()
                            inserted_count += len(records)
                            
                        flash(f'Import thành công {inserted_count} dòng.', 'success')
                except Exception as e: flash(f'Lỗi: {e}', 'danger')

        return redirect(url_for('import_data'))
    
    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    today = datetime.now()
    year, week_num, _ = today.isocalendar()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    default_week_name = f"Tuần {week_num:02d} ({start_of_week.strftime('%d/%m')}-{end_of_week.strftime('%d/%m')})"
    return render_page(CONTENT_TEMPLATE, title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), default_week_name=default_week_name)

@app.route('/sync-rf3g', methods=['POST'])
@login_required
def sync_rf3g():
    if current_user.role != 'admin': return redirect(url_for('index'))
    try:
        db.session.query(RF3G).delete()
        cells = {str(c.cell_code).strip().upper(): c for c in Cell3G.query.all() if c.cell_code}
        configs = {str(c.cell_code).strip().upper(): c for c in Config3G.query.all() if c.cell_code}
        
        all_codes = set(cells.keys()) | set(configs.keys())
        
        rf3g_records = []
        inserted_count = 0
        BATCH_SIZE = 500
        
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
            
            record = RF3G(
                cell_code=code,
                site_code=getattr(c, 'site_code', getattr(cfg, 'site_code', None)),
                ma_node=getattr(c, 'ma_node', getattr(cfg, 'ma_node', None)),
                cell_name=getattr(cfg, 'cell_name', getattr(c, 'ten_tren_he_thong', None)),
                cell_name_alias=getattr(cfg, 'cell_name_alias', None),
                site_name=getattr(cfg, 'site_name', getattr(c, 'ten_loai_tram', None)),
                loai_tram=getattr(c, 'ten_loai_tram', getattr(cfg, 'loai_tram', None)),
                thiet_bi=getattr(c, 'thiet_bi', getattr(cfg, 'thiet_bi', None)),
                tinh_tp=getattr(c, 'tinh_tp', getattr(cfg, 'tinh_tp', None)),
                don_vi_quan_ly=getattr(cfg, 'don_vi_quan_ly', getattr(c, 'ten_don_vi', None)),
                ma_csht=getattr(cfg, 'ma_csht', None),
                csht_site=getattr(c, 'csht_site', None),
                csht_cell=getattr(c, 'csht_cell', None),
                ten_don_vi=getattr(c, 'ten_don_vi', getattr(cfg, 'don_vi_quan_ly', None)),
                latitude=getattr(c, 'latitude', getattr(cfg, 'latitude', None)),
                longitude=getattr(c, 'longitude', getattr(cfg, 'longitude', None)),
                azimuth=getattr(c, 'azimuth', getattr(cfg, 'azimuth', None)),
                mechanical_tilt=getattr(c, 'mechanical_tilt', getattr(cfg, 'mechanical_tilt', None)),
                electrical_tilt=getattr(c, 'electrical_tilt', getattr(cfg, 'electrical_tilt', None)),
                total_tilt=getattr(c, 'total_tilt', getattr(cfg, 'total_tilt', None)),
                antenna_type=getattr(cfg, 'antenna_type', getattr(c, 'loai_anten', None)),
                loai_anten=getattr(c, 'loai_anten', None),
                hang_sx_anten=getattr(c, 'hang_sx_anten', None),
                anten_dai_tan=getattr(c, 'anten_dai_tan', None),
                anten_dung_chung=getattr(c, 'anten_dung_chung', None),
                anten_so_port=getattr(c, 'anten_so_port', None),
                antenna_gain=getattr(c, 'antenna_gain', getattr(cfg, 'antenna_gain', None)),
                antenna_high=getattr(c, 'antenna_high', getattr(cfg, 'antenna_high', None)),
                bang_tan=getattr(c, 'bang_tan', getattr(cfg, 'bang_tan', None)),
                lac=getattr(cfg, 'lac', getattr(c, 'lac', None)),
                ci=getattr(cfg, 'ci', getattr(c, 'ci', None)),
                rac=getattr(cfg, 'rac', None),
                dl_uarfcn=getattr(cfg, 'dl_uarfcn', None),
                dl_psc=getattr(c, 'dl_psc', getattr(cfg, 'dl_psc', None)),
                cpich_power=getattr(cfg, 'cpich_power', getattr(c, 'cpich_power', None)),
                max_power=getattr(cfg, 'max_power', None),
                total_power=getattr(cfg, 'total_power', getattr(c, 'total_power', None)),
                dc_support=getattr(cfg, 'dc_support', None),
                oam_ip=getattr(cfg, 'oam_ip', None),
                cell_type=getattr(cfg, 'cell_type', None),
                no_of_carrier=getattr(cfg, 'no_of_carrier', None),
                special_coverage=getattr(cfg, 'special_coverage', None),
                trang_thai=getattr(c, 'trang_thai', getattr(cfg, 'trang_thai', None)),
                ten_quan_ly=getattr(c, 'ten_quan_ly', None),
                sdt_nguoi_quan_ly=getattr(c, 'sdt_nguoi_quan_ly', None),
                ngay_hoat_dong=getattr(c, 'ngay_hoat_dong', None),
                hoan_canh_ra_doi=getattr(c, 'hoan_canh_ra_doi', None),
                dia_chi=getattr(c, 'dia_chi', None),
                extra_data=json.dumps(merged_extra, ensure_ascii=False) if merged_extra else None
            )
            rf3g_records.append(record)
            
            if len(rf3g_records) >= BATCH_SIZE:
                db.session.bulk_save_objects(rf3g_records)
                db.session.commit()
                inserted_count += len(rf3g_records)
                rf3g_records = []
                gc.collect()
            
        if rf3g_records:
            db.session.bulk_save_objects(rf3g_records)
            db.session.commit()
            inserted_count += len(rf3g_records)
            
        if inserted_count > 0:
            flash(f'Đã ghép nối và đồng bộ {inserted_count} trạm 3G thành công!', 'success')
        else:
            flash('Không có dữ liệu 3G để đồng bộ. Vui lòng kiểm tra lại.', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Lỗi đồng bộ: {str(e)}', 'danger')
    return redirect(url_for('import_data'))

if __name__ == '__main__':
    app.run(debug=True)
