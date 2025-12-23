import React, { useState } from 'react';
import MapComponent from './components/MapComponent';
import Sidebar from './components/Sidebar';
import DataPreview from './components/DataPreview';
import axios from 'axios';

function App() {
  const [bounds, setBounds] = useState(null);
  const [params, setParams] = useState({
    startDate: '2020-01-01',
    endDate: '2024-12-31',
    minDepth: 0,
    maxDepth: 2000,
    type: 'core',
    variables: ['Temp', 'Psal']
  });
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);

  const handleSubmit = async () => {
    if (!bounds) {
      alert('Please select an area on the map first');
      return;
    }

    setLoading(true);
    setPreviewData(null);

    try {
      const response = await axios.post('http://localhost:8000/api/process', {
        bounds,
        params
      }, { responseType: 'blob' });

      // Parse Blob for Preview
      const text = await response.data.text();
      setPreviewData(text);

      // Create download link
      const url = window.URL.createObjectURL(response.data);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `argo_${params.type}_data_${Date.now()}.csv`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      
      setLoading(false);
    } catch (error) {
      console.error('Error:', error);
      alert('Failed to process data: ' + (error.response?.data?.error || error.message));
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <Sidebar
        bounds={bounds}
        params={params}
        setParams={setParams}
        onSubmit={handleSubmit}
      />
      <div className="map-container">
        <MapComponent onBoundsChange={setBounds} />
        {previewData && (
          <DataPreview 
            csvData={previewData} 
            onClose={() => setPreviewData(null)} 
          />
        )}
        {loading && (
          <div className="loader-overlay">
            <div className="loader-content">
              <div className="loader-spinner"></div>
              <div className="loader-text">Retrieving Accurate Argo Data...</div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
