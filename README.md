# Argo Nexus - Ocean Data Analysis Platform

## Overview
Argo Nexus is a modern web application for querying, visualizing, and downloading Argo float data. It features a high-performance **FastAPI** backend that performs optimized binary searches on the global Argo profile index and a premium **React** frontend with a "Space Command" aesthetic.

## Features
- **Fast Search**: Optimized binary search algorithm for instant date-range filtering.
- **Interactive Map**: Geographic bounds selection using a custom Leaflet map.
- **Accurate Data**: Downloads authentic NetCDF files from IFREMER and extracts data with full QC flag support.
- **Data Persistence**: Automatically manages a local library of downloaded NetCDF files.

## Project Structure
```
incois-project/
├── backend/                # FastAPI Python Backend
│   ├── main.py            # Main application logic
│   ├── requirements.txt   # Python dependencies
│   └── downloads/         # Directory for downloaded .nc files (Ignored by Git)
├── frontend/               # React Frontend
│   ├── src/               # Source code
│   └── public/            # Static files
└── ar_index_global_prof.txt # Global Argo Index (Ignored by Git)
```

## Setup Instructions

### 1. Prerequisites
- **Node.js** (for Frontend)
- **Python 3.10+** (for Backend)

### 2. Backend Setup
Navigate to the `backend` directory:
```bash
cd backend
python -m venv venv
# Windows:
.\venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Frontend Setup
Navigate to the `frontend` directory:
```bash
cd frontend
npm install
```

## Usage

1.  **Start the Backend**:
    From the `backend` folder:
    ```bash
    uvicorn main:app --reload
    ```
    The server will start on `http://localhost:8000`. It will automatically fetch the Argo index file if not present.

2.  **Start the Frontend**:
    From the `frontend` folder:
    ```bash
    npm start
    ```
    Open `http://localhost:3000` in your browser.

3.  **Perform a Search**:
    - Draw a rectangle on the map to define the geographic bounds.
    - Select a Date Range and Depth Range.
    - Click **"Execute Search & Download"**.

## Important Notes
- **Downloads Directory**: The backend will automatically create a `downloads/` directory inside `backend/` to store the raw `.nc` files fetched from the Argo servers. **Do not commit this directory to Git.**
- **Accuracy**: The generated CSV includes QC flags (e.g., `TEMP_QC`) to ensure 100% data transparency and accuracy.
