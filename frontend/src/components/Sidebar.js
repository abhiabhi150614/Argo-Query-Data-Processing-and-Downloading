import React from 'react';

const Sidebar = ({ bounds, params, setParams, onSubmit, onBoundsChange }) => {
  const handleChange = (e) => {
    const { name, value } = e.target;
    setParams({ ...params, [name]: value });
  };

  const handleTypeChange = (type) => {
    setParams({ ...params, type });
  };

  const handleBoundsChange = (e) => {
    const { name, value } = e.target;
    if (value === '') {
      onBoundsChange({ ...bounds, [name]: '' });
      return;
    }
    const numValue = parseFloat(value);
    if (!isNaN(numValue)) {
      onBoundsChange({ ...bounds, [name]: numValue });
    }
  };

  const coreVars = ['TEMP', 'PSAL', 'PRES', 'All QC Flags', 'All Available Parameters'];
  const bioVars = ['CHLA', 'DOXY', 'NITRATE', 'PH', 'BBP700', 'IRRADIANCE', 'TEMP', 'PSAL', 'PRES', 'All QC Flags', 'All Available Parameters'];
  const currentVars = params.type === 'core' ? coreVars : bioVars;

  return (
    <div className="sidebar">
      <div className="header">
        <div className="logo">ARGO NEXUS</div>
        <div className="status-badge">
          <div className="status-dot"></div>
          Live Connection
        </div>
      </div>

      <div className="section">
        <div className="section-title">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/>
            <circle cx="12" cy="10" r="3"/>
          </svg>
          Target Zone
        </div>
        <div className="grid-2">
          <div className="input-group">
            <span className="input-label">North</span>
            <input
              type="number"
              step="0.0001"
              name="north"
              className="input-field"
              value={bounds?.north || ''}
              onChange={handleBoundsChange}
              placeholder="0.0000"
            />
          </div>
          <div className="input-group">
            <span className="input-label">South</span>
            <input
              type="number"
              step="0.0001"
              name="south"
              className="input-field"
              value={bounds?.south || ''}
              onChange={handleBoundsChange}
              placeholder="0.0000"
            />
          </div>
        </div>
        <div className="grid-2">
          <div className="input-group">
            <span className="input-label">West</span>
            <input
              type="number"
              step="0.0001"
              name="west"
              className="input-field"
              value={bounds?.west || ''}
              onChange={handleBoundsChange}
              placeholder="0.0000"
            />
          </div>
          <div className="input-group">
            <span className="input-label">East</span>
            <input
              type="number"
              step="0.0001"
              name="east"
              className="input-field"
              value={bounds?.east || ''}
              onChange={handleBoundsChange}
              placeholder="0.0000"
            />
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <rect x="3" y="4" width="18" height="18" rx="2" ry="2"/>
            <line x1="16" y1="2" x2="16" y2="6"/>
            <line x1="8" y1="2" x2="8" y2="6"/>
            <line x1="3" y1="10" x2="21" y2="10"/>
          </svg>
          Temporal Range
        </div>
        <div className="grid-2">
          <div className="input-group">
            <span className="input-label">Start Date</span>
            <input
              type="date"
              name="startDate"
              className="input-field"
              value={params.startDate}
              onChange={handleChange}
            />
          </div>
           <div className="input-group">
            <span className="input-label">End Date</span>
             <input
              type="date"
              name="endDate"
              className="input-field"
              value={params.endDate}
              onChange={handleChange}
            />
          </div>
        </div>
      </div>

       <div className="section">
        <div className="section-title">
          
          Depth Range (Meters)
        </div>
        <div className="grid-2">
          <div className="input-group">
            <span className="input-label">Min</span>
            <input
              type="number"
              name="minDepth"
              className="input-field"
              value={params.minDepth}
              onChange={handleChange}
              placeholder="0"
            />
          </div>
          <div className="input-group">
            <span className="input-label">Max</span>
            <input
              type="number"
              name="maxDepth"
              className="input-field"
              value={params.maxDepth}
              onChange={handleChange}
              placeholder="2000"
            />
          </div>
        </div>
      </div>
      
       <div className="section">
        <div className="section-title">
          
          Search Type
        </div>
        <div className="type-selector">
          <button
            className={`type-btn ${params.type === 'core' ? 'active' : ''}`}
            onClick={() => handleTypeChange('core')}
          >
            Core Argo
          </button>
          <button
            className={`type-btn ${params.type === 'bio' ? 'active' : ''}`}
            onClick={() => handleTypeChange('bio')}
          >
            BGC Argo
          </button>
        </div>
      </div>

      <div className="section">
        <div className="section-title">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
          </svg>
          Parameters Included (Complete NetCDF Export)
        </div>
        <div className="variables">
          {currentVars.map(v => (
            <div key={v} className="var-chip active">
              {v}
            </div>
          ))}
        </div>
        <div className="info-note">
          ℹ️ All available parameters from NetCDF files will be exported
        </div>
      </div>

      <button className="btn-primary" onClick={onSubmit}>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
          <polyline points="7 10 12 15 17 10"/>
          <line x1="12" y1="15" x2="12" y2="3"/>
        </svg>
        Execute Search & Download
      </button>

      <div className="info-text">
        System 100% Optimized • Cost: ₹0 • Automated Retrieval
      </div>
    </div>
  );
};

export default Sidebar;
