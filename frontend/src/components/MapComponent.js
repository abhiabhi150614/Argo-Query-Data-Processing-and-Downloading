import React, { useRef, useEffect } from 'react';
import { MapContainer, TileLayer, FeatureGroup } from 'react-leaflet';
import { EditControl } from 'react-leaflet-draw';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import 'leaflet-draw/dist/leaflet.draw.css';

import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
  iconUrl: icon,
  shadowUrl: iconShadow,
  iconSize: [25, 41],
  iconAnchor: [12, 41]
});

L.Marker.prototype.options.icon = DefaultIcon;

const MapComponent = ({ onBoundsChange, bounds }) => {
  const mapRef = useRef();
  const featureGroupRef = useRef();

  useEffect(() => {
    if (bounds && featureGroupRef.current && mapRef.current && 
        typeof bounds.north === 'number' && typeof bounds.south === 'number' &&
        typeof bounds.east === 'number' && typeof bounds.west === 'number') {
      // Clear existing layers
      featureGroupRef.current.clearLayers();
      
      // Validate bounds before creating rectangle
      if (bounds.north > bounds.south && bounds.east > bounds.west) {
        // Create editable rectangle from bounds
        const rectangle = L.rectangle([
          [bounds.south, bounds.west],
          [bounds.north, bounds.east]
        ], {
          color: '#0284C7',
          weight: 2,
          fillOpacity: 0.2
        });
        
        // Make rectangle editable
        rectangle.editing.enable();
        
        // Add event listener for when rectangle is edited
        rectangle.on('edit', (e) => {
          const editedBounds = e.target.getBounds();
          onBoundsChange({
            north: editedBounds.getNorth(),
            south: editedBounds.getSouth(),
            east: editedBounds.getEast(),
            west: editedBounds.getWest()
          });
        });
        
        featureGroupRef.current.addLayer(rectangle);
      }
    }
  }, [bounds, onBoundsChange]);

  const onCreate = (e) => {
    const { layerType, layer } = e;
    if (layerType === 'rectangle') {
      const bounds = layer.getBounds();
      onBoundsChange({
        north: bounds.getNorth(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        west: bounds.getWest()
      });
    }
  };

  const onEdited = (e) => {
    const { layers } = e;
    layers.eachLayer((layer) => {
      const bounds = layer.getBounds();
      onBoundsChange({
        north: bounds.getNorth(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        west: bounds.getWest()
      });
    });
  };

  return (
    <div className="map-container">
      <MapContainer
        center={[20, 0]}
        zoom={2}
        style={{ height: '100vh', width: '100%' }}
        ref={mapRef}
        whenCreated={(mapInstance) => {
          mapRef.current = mapInstance;
        }}
      >
        <TileLayer
          url="https://server.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}"
          attribution='Tiles &copy; Esri &mdash; Sources: GEBCO, NOAA, CHS, OSU, UNH, CSUMB, National Geographic, DeLorme, NAVTEQ, and Esri'
        />
        <FeatureGroup ref={featureGroupRef}>
          <EditControl
            position="topright"
            onCreated={onCreate}
            onEdited={onEdited}
            draw={{
              rectangle: true,
              polyline: false,
              polygon: false,
              circle: false,
              marker: false,
              circlemarker: false
            }}
            edit={{
              edit: true,
              remove: true
            }}
          />
        </FeatureGroup>
      </MapContainer>
    </div>
  );
};

export default MapComponent;
