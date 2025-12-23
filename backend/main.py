from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
import pandas as pd
import numpy as np
import io
import os
import bisect
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
    
    print("Loading index file...")
    
    # Check local first
    content = ""
    if os.path.exists(LOCAL_INDEX_PATH):
        print("Reading local index...")
        with open(LOCAL_INDEX_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        print("Downloading index...")
        async with httpx.AsyncClient() as client:
            resp = await client.get(REMOTE_INDEX_URL)
            content = resp.text
            
    # Parse CSV-like structure
    # Skip comments
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
    # Sort by date for binary search
    DATE_SORTED_PROFILES = sorted(data, key=lambda x: x['date'])
    print(f"Index loaded: {len(CACHED_PROFILES)} profiles.")

@app.on_event("startup")
async def startup_event():
    await load_index()

def binary_search_date_range(start_date, end_date):
    """Effectively finds the slice of profiles within the date range."""
    # Dates in index are YYYYMMDDHHMMSS
    # Inputs are YYYY-MM-DD
    start_str = start_date.replace('-', '') + "000000"
    end_str = end_date.replace('-', '') + "235959"
    
    dates = [x['date'] for x in DATE_SORTED_PROFILES]
    
    left_idx = bisect.bisect_left(dates, start_str)
    right_idx = bisect.bisect_right(dates, end_str)
    
    return DATE_SORTED_PROFILES[left_idx:right_idx]

async def download_netcdf(file_path):
    """Downloads NetCDF file and saves it to the downloads directory."""
    url = f"https://data-argo.ifremer.fr/dac/{file_path}"
    
    # Create local path
    filename = os.path.basename(file_path)
    local_path = os.path.join(DOWNLOADS_DIR, filename)
    
    # If already exists, return existing path (caching)
    if os.path.exists(local_path):
        return local_path
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=60.0)
        resp.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(resp.content)
            
        return local_path

def process_netcdf(file_path, params):
    """Extracts data from NetCDF file using xarray with High Accuracy."""
    try:
        ds = xr.open_dataset(file_path)
        
        data = []
        
        # We need PRES
        if 'PRES' not in ds:
            ds.close()
            return []
            
        pres = ds['PRES'].values
        
        # Define extraction variables based on type
        # We include QC flags for "100% accurate" representation
        vars_to_extract = {}
        flags_to_extract = {}
        
        if params.type == 'core':
            target_vars = ['TEMP', 'PSAL']
        else:
            target_vars = ['CHLA', 'DOXY', 'NITRATE', 'PH_IN_SITU_TOTAL', 'BBP700', 'DOWN_IRRADIANCE412']

        for v in target_vars:
            if v in ds:
                vars_to_extract[v] = ds[v].values
                qc_key = f"{v}_QC"
                if qc_key in ds:
                    flags_to_extract[qc_key] = ds[qc_key].values

        # Iterate profiles 
        # Safety for dimensions
        if pres.ndim == 2:
            n_prof, n_levels = pres.shape
            for p in range(n_prof):
                for l in range(n_levels):
                    p_val = pres[p, l]
                    if np.isnan(p_val): continue
                    
                    depth = float(p_val) # approx pressure to depth 1:1 for simplicity or apply func
                    
                    if params.minDepth <= depth <= params.maxDepth:
                        row = {'depth': depth}
                        
                        # Data Values
                        for vname, vdata in vars_to_extract.items():
                            val = vdata[p, l]
                            row[vname] = float(val) if not np.isnan(val) else ''
                            
                        # QC Flags
                        for qname, qdata in flags_to_extract.items():
                            val = qdata[p, l]
                            # QC flags are often bytes or chars
                            if isinstance(val, (bytes, np.bytes_)):
                                row[qname] = val.decode('utf-8')
                            else:
                                row[qname] = str(val)

                        data.append(row)
        
        ds.close()
        return data

    except Exception as e:
        print(f"Error processing NetCDF {file_path}: {e}")
        return []

@app.post("/api/process")
async def process_data(req: ProcessRequest):
    await load_index()
    
    # Debug Request
    print(f"Received Request: {req}")
    print(f"Bounds: N={req.bounds.north}, S={req.bounds.south}, E={req.bounds.east}, W={req.bounds.west}")
    
    # 1. Date Filter (Binary Search)
    candidates = binary_search_date_range(req.params.startDate, req.params.endDate)
    print(f"Date filter found {len(candidates)} candidates.")
    
    # 2. Geo Filter
    filtered = [
        p for p in candidates
        if req.bounds.south <= p['lat'] <= req.bounds.north and
           req.bounds.west <= p['lon'] <= req.bounds.east
    ]
    print(f"Geo filter reduced to {len(filtered)} profiles.")
    
    if not filtered:
        raise HTTPException(status_code=404, detail="No profiles found in selection.")
        
    # Limit to 20 for performance (or higher if desktop app usage)
    selection = filtered[:20] 
    
    all_results = []
    
    for profile in selection:
        try:
            print(f"Processing {profile['file']}...")
            local_path = await download_netcdf(profile['file'])
            extracted = process_netcdf(local_path, req.params)
            
            for row in extracted:
                row.update({
                    'date': profile['date'],
                    'lat': profile['lat'],
                    'lon': profile['lon'],
                    'file': profile['file'],
                    'local_path': local_path
                })
                all_results.append(row)
        except Exception as e:
            print(f"Failed to process {profile['file']}: {e}")
            
    if not all_results:
        raise HTTPException(status_code=404, detail="No valid data extracted.")
        
    df = pd.DataFrame(all_results)
    
    # Reorder columns for "Proper Format"
    base_cols = ['date', 'lat', 'lon', 'depth', 'file']
    data_cols = [c for c in df.columns if c not in base_cols and 'QC' not in c and c != 'local_path']
    qc_cols = [c for c in df.columns if 'QC' in c]
    
    final_order = base_cols + data_cols + qc_cols
    df = df.reindex(columns=final_order)
    
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=argo_data.csv"
    
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
