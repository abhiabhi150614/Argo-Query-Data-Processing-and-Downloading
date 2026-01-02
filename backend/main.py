from fastapi import FastAPI, HTTPException, Body, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
import pandas as pd
import numpy as np
import io
import os
import bisect
import json
import re
from typing import List, Optional
from pydantic import BaseModel
import xarray as xr

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
LOCAL_INDEX_PATH = '../ar_index_global_prof.txt'
REMOTE_INDEX_URL = 'https://data-argo.ifremer.fr/ar_index_global_prof.txt'
DOWNLOADS_DIR = 'downloads'

# Ensure downloads directory exists
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# In-memory Cache
CACHED_PROFILES = []
DATE_SORTED_PROFILES = []

class SearchParams(BaseModel):
    startDate: str
    endDate: str
    minDepth: float
    maxDepth: float
    type: str # 'core' or 'bio'

class Bounds(BaseModel):
    north: float
    south: float
    east: float
    west: float

class ProcessRequest(BaseModel):
    bounds: Bounds
    params: SearchParams

async def load_index():
    """Loads the index file into memory and sorts it for binary search."""
    global CACHED_PROFILES, DATE_SORTED_PROFILES
    
    if CACHED_PROFILES:
        return
    
    # Check local first
    content = ""
    if os.path.exists(LOCAL_INDEX_PATH):
        with open(LOCAL_INDEX_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        async with httpx.AsyncClient() as client:
            resp = await client.get(REMOTE_INDEX_URL)
            content = resp.text
            
    # Parse CSV-like structure
    lines = [line for line in content.splitlines() if not line.startswith('#') and 'file,' not in line]
    
    data = []
    for line in lines:
        parts = line.split(',')
        if len(parts) >= 8:
            try:
                data.append({
                    'file': parts[0],
                    'date': parts[1],
                    'lat': float(parts[2]),
                    'lon': float(parts[3]),
                    'ocean': parts[4],
                    'profiler_type': parts[5],
                    'institution': parts[6],
                    'date_update': parts[7]
                })
            except ValueError:
                continue
            
    CACHED_PROFILES = data
    DATE_SORTED_PROFILES = sorted(data, key=lambda x: x['date'])

@app.on_event("startup")
async def startup_event():
    await load_index()

def binary_search_date_range(start_date, end_date):
    """Effectively finds the slice of profiles within the date range."""
    start_str = start_date.replace('-', '') + "000000"
    end_str = end_date.replace('-', '') + "235959"
    dates = [x['date'] for x in DATE_SORTED_PROFILES]
    left_idx = bisect.bisect_left(dates, start_str)
    right_idx = bisect.bisect_right(dates, end_str)
    return DATE_SORTED_PROFILES[left_idx:right_idx]

async def download_netcdf(file_path):
    """Downloads NetCDF file and saves it to the downloads directory."""
    url = f"https://data-argo.ifremer.fr/dac/{file_path}"
    filename = os.path.basename(file_path)
    local_path = os.path.join(DOWNLOADS_DIR, filename)
    
    if os.path.exists(local_path):
        return local_path
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=60.0)
        resp.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(resp.content)
        return local_path

def process_netcdf(file_path, params):
    """Extracts ALL data from NetCDF file using xarray with High Accuracy."""
    try:
        ds = xr.open_dataset(file_path)
        data = []
        if 'PRES' not in ds:
            ds.close()
            return []
        pres = ds['PRES'].values
        
        # Extract ALL variables from NetCDF file
        vars_to_extract = {}
        flags_to_extract = {}
        
        # Get all data variables (excluding dimensions and coordinates)
        for var_name in ds.data_vars:
            if var_name not in ['PRES'] and not var_name.endswith('_QC'):
                vars_to_extract[var_name] = ds[var_name].values
                qc_key = f"{var_name}_QC"
                if qc_key in ds:
                    flags_to_extract[qc_key] = ds[qc_key].values
        
        # Always include PRES
        vars_to_extract['PRES'] = pres
        if 'PRES_QC' in ds:
            flags_to_extract['PRES_QC'] = ds['PRES_QC'].values

        if pres.ndim == 2:
            n_prof, n_levels = pres.shape
            for p in range(n_prof):
                for l in range(n_levels):
                    p_val = pres[p, l]
                    if np.isnan(p_val): continue
                    
                    depth = float(p_val)
                    if params.minDepth <= depth <= params.maxDepth:
                        row = {'depth': depth}
                        for vname, vdata in vars_to_extract.items():
                            if vdata.ndim >= 2:
                                val = vdata[p, l] if vdata.shape[0] > p and vdata.shape[1] > l else np.nan
                            else:
                                val = vdata[p] if vdata.shape[0] > p else np.nan
                            row[vname] = float(val) if not np.isnan(val) else ''
                        for qname, qdata in flags_to_extract.items():
                            if qdata.ndim >= 2:
                                val = qdata[p, l] if qdata.shape[0] > p and qdata.shape[1] > l else ''
                            else:
                                val = qdata[p] if qdata.shape[0] > p else ''
                            if isinstance(val, (bytes, np.bytes_)):
                                row[qname] = val.decode('utf-8')
                            else:
                                row[qname] = str(val)
                        data.append(row)
        ds.close()
        return data
    except Exception as e:
        print(f"Error processing NetCDF: {e}")
        return []

def extract_metadata(filename):
    """Extracts Platform and Cycle from filename e.g. R1901839_334.nc"""
    match = re.search(r'([A-Z]*)([0-9]+)_([0-9]+D?)', filename)
    if match:
        return match.group(2), match.group(3)
    return "", ""

@app.websocket("/api/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        data = await websocket.receive_text()
        req_dict = json.loads(data)
        
        # Manually reconstruct objects (Pydantic models need explicit dict unpacking if nested)
        # But simpler to just use dict access here for safety
        bounds = req_dict['bounds']
        params = req_dict['params']
        
        # Params object wrapper for helper functions
        class ParamsObj:
            def __init__(self, d): self.__dict__ = d
        params_obj = ParamsObj(params)

        await websocket.send_json({"type": "log", "message": "Initializing Search..."})
        await load_index()
        
        # 1. Date Filter
        candidates = binary_search_date_range(params['startDate'], params['endDate'])
        await websocket.send_json({"type": "log", "message": f"Found {len(candidates)} profiles in date range."})
        
        # 2. Geo Filter
        filtered = [
            p for p in candidates
            if bounds['south'] <= p['lat'] <= bounds['north'] and
               bounds['west'] <= p['lon'] <= bounds['east']
        ]
        await websocket.send_json({"type": "log", "message": f"Geographic filter reduced to {len(filtered)} profiles."})
        
        if not filtered:
            await websocket.send_json({"type": "error", "message": "No profiles found in selected area/date."})
            return

        selection = filtered
        all_results = []
        
        for i, profile in enumerate(selection):
            try:
                msg = f"[{i+1}/{len(selection)}] Processing {profile['file']}..."
                await websocket.send_json({"type": "log", "message": msg})
                
                local_path = await download_netcdf(profile['file'])
                extracted = process_netcdf(local_path, params_obj)
                
                filename = os.path.basename(profile['file'])
                platform, cycle = extract_metadata(filename)
                
                for row in extracted:
                    row.update({
                        'Date': profile['date'],
                        'Latitude': profile['lat'],
                        'Longitude': profile['lon'],
                        'Platform': platform,
                        'Cycle': cycle,
                        'Institution': profile.get('institution', ''),
                        'Ocean': profile.get('ocean', ''),
                        'File': filename
                    })
                    all_results.append(row)
            except Exception as e:
                await websocket.send_json({"type": "log", "message": f"Error on {profile['file']}: {str(e)}"})
        
        if not all_results:
            await websocket.send_json({"type": "error", "message": "No data extracted from profiles."})
            return
            
        await websocket.send_json({"type": "log", "message": "Generating Excel-compatible CSV..."})
        
        df = pd.DataFrame(all_results)
        
        # Proper Column Ordering with enhanced metadata
        first_cols = ['Platform', 'Cycle', 'Date', 'Latitude', 'Longitude', 'depth']
        # Get all parameter columns (excluding QC and metadata)
        param_cols = [c for c in df.columns if c not in first_cols and 'QC' not in c and c not in ['File', 'Institution', 'Ocean']]
        qc_cols = [c for c in df.columns if 'QC' in c]
        meta_cols = ['Institution', 'Ocean', 'File'] if any(c in df.columns for c in ['Institution', 'Ocean', 'File']) else ['File']
        
        final_cols = first_cols + param_cols + qc_cols + meta_cols
        
        # Handle missing cols if any
        existing_cols = [c for c in final_cols if c in df.columns]
        df = df[existing_cols]
        
        # Rename for niceness
        df.rename(columns={'depth': 'Depth (dbar)'}, inplace=True)
        
        # CSV String with BOM for Excel
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False) # standard encoding
        
        # Combine BOM + CSV string
        csv_content = csv_buffer.getvalue()
        
        await websocket.send_json({
            "type": "complete", 
            "csv": csv_content,
            "filename": f"argo_complete_dataset_{params['type']}_{len(selection)}_profiles.csv"
        })
        
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"WS Error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
