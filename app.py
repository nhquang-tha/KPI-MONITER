import os
import jinja2
import pandas as pd
import json
import gc
import re
import zipfile
import unicodedata
from io import BytesIO
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
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024  # 32MB max upload

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
    # 1. Chuyển về chuỗi, xóa khoảng trắng đầu cuối
    col_name = str(col_name).strip()
    
    # 2. Map các trường hợp đặc biệt (Case sensitive keys)
    special_map = {
        'ENodeBID': 'enodeb_id', 'gNodeB ID': 'gnodeb_id', 'GNODEB_ID': 'gnodeb_id',
        'CELL_ID': 'cell_id', 'SITE_NAME': 'site_name', 'Frenquency': 'frequency',
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
        'CQI_5G': 'cqi_5g', 'CQI_4G': 'cqi_4g'
    }
    if col_name in special_map:
        return special_map[col_name]

    # 3. Xử lý tiếng Việt và ký tự lạ
    # Bỏ dấu tiếng Việt
    no_accent = remove_accents(col_name)
    # Chuyển về chữ thường
    lower = no_accent.lower()
    # Thay thế khoảng trắng và ký tự không phải chữ/số bằng gạch dưới
    clean = re.sub(r'[^a-z0-9]', '_', lower)
    # Xóa gạch dưới kép nếu có
    clean = re.sub(r'_+', '_', clean)
    
    # Map lại các cột phổ biến sau khi clean
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo',
        'ten_cell': 'ten_cell', 'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap',
        'cell_name': 'cell_name', 'cell_code': 'cell_code', 'site_code': 'site_code',
        'anten_height': 'anten_height', 'total_tilt': 'total_tilt',
        'traffic_vol_dl': 'traffic_vol_dl', 'res_blk_dl': 'res_blk_dl',
        'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges'
    }
    
    return common_map.get(clean, clean)

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
    poi_name = db.Column(db.String(200))

class POI5G(db.Model):
    __tablename__ = 'poi_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200))

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
    # Thêm các cột phụ để tránh lỗi nếu file CSV có
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
    # Phụ
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
    # Phụ
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

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '3g')
    action = request.args.get('action')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech, RF3G)
    
    if action == 'export':
        def generate():
            yield '\ufeff'.encode('utf-8')
            cols = [c.key for c in Model.__table__.columns]
            yield (','.join(cols) + '\n').encode('utf-8')
            query = db.select(Model).execution_options(yield_per=100)
            for row in db.session.execute(query).scalars():
                yield (','.join([str(getattr(row, c, '') or '').replace(',', ';') for c in cols]) + '\n').encode('utf-8')
        return Response(stream_with_context(generate()), mimetype='text/csv', headers={"Content-Disposition": f"attachment; filename=RF_{tech}.csv"})

    # Dynamic columns
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    rows = Model.query.limit(500).all()
    data = [{c: getattr(r, c) for c in cols} | {'id': r.id} for r in rows]
    
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
                    chunks = pd.read_csv(file, chunksize=2000)
                else:
                    df = pd.read_excel(file)
                    chunks = [df]
                
                for df in chunks:
                    # Clean columns
                    df.columns = [clean_header(c) for c in df.columns]
                    
                    # Validate
                    if not all(r in df.columns for r in req_cols):
                        flash(f'File {file.filename} thiếu cột bắt buộc: {req_cols}', 'danger')
                        break
                    
                    # Clean data & Insert
                    records = []
                    for row in df.to_dict('records'):
                        clean_row = {}
                        for k, v in row.items():
                            if k in valid_db_columns:
                                val = v
                                if pd.isna(val): val = None
                                elif isinstance(val, str): val = val.strip() # TRIM WHITESPACE
                                clean_row[k] = val
                                
                        # KPI Specific: Fallback for traffic column
                        if 'kpi' in itype and 'traffic' in valid_db_columns and 'traffic' not in clean_row:
                             if 'traffic_vol_dl' in clean_row: clean_row['traffic'] = clean_row['traffic_vol_dl']
                        
                        records.append(clean_row)
                    
                    if records:
                        db.session.bulk_insert_mappings(Model, records)
                        db.session.commit()
                        count += len(records)
                        
            except Exception as e:
                db.session.rollback()
                flash(f'Lỗi import {file.filename}: {e}', 'danger')
        
        if count > 0: flash(f'Đã import {count} dòng.', 'success')
        return redirect(url_for('import_data'))

    # Fetch dates for KPI list
    dates = []
    try:
        for M, label in [(KPI3G, '3G'), (KPI4G, '4G'), (KPI5G, '5G')]:
            ds = db.session.query(M.thoi_gian).distinct().all()
            dates.extend([{'type': f'KPI {label}', 'date': d[0]} for d in ds])
        dates.sort(key=lambda x: x['date'], reverse=True)
    except: pass
    
    return render_page(CONTENT_TEMPLATE, title="Import", active_page='import', imported_kpi_dates=dates)

@app.route('/kpi')
@login_required
def kpi():
    tech = request.args.get('tech', '3g')
    cell_input = request.args.get('cell_name', '').strip()
    charts = {}
    
    if cell_input:
        KModel = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
        RModel = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
        
        # Find cells from input (Site code or Cell list)
        cells = [c.strip() for c in re.split(r'[,\s]+', cell_input) if c.strip()]
        
        # If input looks like a Site Code (usually shorter, distinct pattern), try finding child cells
        # Assuming Site Code doesn't contain commas
        if len(cells) == 1:
            site_cells = RModel.query.filter(RModel.site_code == cells[0]).all()
            if site_cells:
                cells = [c.cell_code for c in site_cells]

        if cells:
            data = KModel.query.filter(KModel.ten_cell.in_(cells)).all()
            # Sort by date
            try: data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
            except: pass
            
            if data:
                all_dates = sorted(list(set(d.thoi_gian for d in data)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                grouped = defaultdict(list)
                for d in data: grouped[d.ten_cell].append(d)
                
                # Chart Configs
                cfgs = {
                    '3g': [('traffic', 'Traffic'), ('cssr', 'CSSR'), ('csconges', 'CS Congestion')],
                    '4g': [('traffic', 'Traffic'), ('user_dl_avg_thput', 'Thput DL'), ('cqi_4g', 'CQI')],
                    '5g': [('traffic', 'Traffic'), ('user_dl_avg_throughput', 'Thput DL'), ('cqi_5g', 'CQI')]
                }.get(tech, [])
                
                colors = ['#007bff', '#28a745', '#dc3545', '#ffc107', '#17a2b8']
                
                for key, label in cfgs:
                    ds = []
                    for i, (cell, rows) in enumerate(grouped.items()):
                        row_map = {r.thoi_gian: getattr(r, key) for r in rows}
                        vals = [row_map.get(d, None) for d in all_dates]
                        ds.append({'label': cell, 'data': vals, 'borderColor': colors[i % 5], 'fill': False})
                    
                    charts[key] = {'title': label, 'labels': all_dates, 'datasets': ds}

    return render_page(CONTENT_TEMPLATE, title="KPI Chart", active_page='kpi', charts=charts, selected_tech=tech, cell_name_input=cell_input)

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
