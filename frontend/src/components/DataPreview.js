import React from 'react';

const DataPreview = ({ csvData, onClose }) => {
  if (!csvData) return null;

  // Simple CSV Parser
  const rows = csvData.trim().split('\n');
  const headers = rows[0].split(',');
  const dataRows = rows.slice(1, 11); // Show snippet

  return (
    <div className="preview-container">
      <div className="preview-header">
        <div className="preview-title">Data Accuracy Verification (First 10 Rows)</div>
        <button onClick={onClose} className="type-btn active" style={{width: 'auto', padding: '0.5rem 1rem'}}>
          Close Preview
        </button>
      </div>
      <table className="data-table">
        <thead>
          <tr>
            {headers.map((h, i) => <th key={i}>{h}</th>)}
          </tr>
        </thead>
        <tbody>
          {dataRows.map((row, i) => (
            <tr key={i}>
              {row.split(',').map((cell, j) => <td key={j}>{cell}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default DataPreview;
