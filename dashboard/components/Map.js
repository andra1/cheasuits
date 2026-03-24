import { useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

const CASE_COLORS = {
  FC: '#dc2626',
  CV: '#2563eb',
  CH: '#ca8a04',
};

const DEFAULT_COLOR = '#6b7280';

function getColor(caseType) {
  return CASE_COLORS[caseType] || DEFAULT_COLOR;
}

export default function Map({ features }) {
  return (
    <MapContainer
      center={[38.52, -89.93]}
      zoom={10}
      style={{ height: '100%', width: '100%' }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {features.map((f) => (
        <CircleMarker
          key={f.document_number}
          center={[f.lat, f.lng]}
          radius={8}
          fillColor={getColor(f.case_type)}
          fillOpacity={0.8}
          color="#fff"
          weight={1.5}
        >
          <Popup>
            <div className="text-sm leading-relaxed">
              <div className="font-bold text-base mb-1">{f.case_number}</div>
              <div><span className="font-semibold">Type:</span> {f.case_type || 'N/A'}</div>
              <div><span className="font-semibold">Filed:</span> {f.recorded_date}</div>
              <div><span className="font-semibold">Party:</span> {f.party2}</div>
              <div><span className="font-semibold">Parcel:</span> {f.parcel_id}</div>
              {f.subdivision && (
                <div><span className="font-semibold">Subdivision:</span> {f.subdivision}</div>
              )}
              <div className="mt-1 text-xs text-gray-500">Doc #{f.document_number}</div>
            </div>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
}
