const CASE_COLORS = {
  FC: { color: '#dc2626', label: 'Foreclosure' },
  CV: { color: '#2563eb', label: 'Civil' },
  CH: { color: '#ca8a04', label: 'Chancery' },
  other: { color: '#6b7280', label: 'Other' },
};

export default function Sidebar({ data }) {
  const { summary, date_range, total_records, geocoded_count, generated_at } = data;
  const fcCount = summary.FC || 0;
  const fcPct = total_records > 0 ? ((fcCount / total_records) * 100).toFixed(0) : 0;

  return (
    <aside className="w-72 bg-gray-900 text-gray-100 p-5 flex flex-col gap-5 overflow-y-auto shrink-0">
      <div>
        <h1 className="text-lg font-bold leading-tight">Lis Pendens Map</h1>
        <p className="text-xs text-gray-400 mt-0.5">St. Clair County, IL</p>
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <div className="text-3xl font-bold">{total_records}</div>
        <div className="text-sm text-gray-400">Total Filings</div>
        <div className="mt-2 text-sm">
          <span className="text-red-400 font-semibold">{fcCount} FC</span>
          <span className="text-gray-500 ml-1">({fcPct}%)</span>
        </div>
      </div>

      <div className="bg-gray-800 rounded-lg p-4 text-sm">
        <div className="text-xs text-gray-400 uppercase tracking-wide mb-2">Date Range</div>
        <div>{date_range.earliest || '—'}</div>
        <div className="text-gray-500">to</div>
        <div>{date_range.latest || '—'}</div>
      </div>

      <div className="bg-gray-800 rounded-lg p-4 text-sm">
        <div className="text-xs text-gray-400 uppercase tracking-wide mb-2">Geocoding</div>
        <div>
          {geocoded_count}/{total_records} mapped
        </div>
        {total_records > geocoded_count && (
          <div className="text-xs text-yellow-500 mt-1">
            {total_records - geocoded_count} not on map
          </div>
        )}
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <div className="text-xs text-gray-400 uppercase tracking-wide mb-2">Legend</div>
        <div className="flex flex-col gap-1.5">
          {Object.entries(CASE_COLORS).map(([key, { color, label }]) => {
            const count = summary[key] || 0;
            if (key !== 'other' && count === 0) return null;
            if (key === 'other' && !summary.other) return null;
            return (
              <div key={key} className="flex items-center gap-2 text-sm">
                <span
                  className="inline-block w-3 h-3 rounded-full shrink-0"
                  style={{ backgroundColor: color }}
                />
                <span>{label}</span>
                <span className="text-gray-500 ml-auto">{count}</span>
              </div>
            );
          })}
        </div>
      </div>

      {generated_at && (
        <div className="text-xs text-gray-500 mt-auto">
          Updated: {generated_at.replace('T', ' ')}
        </div>
      )}
    </aside>
  );
}
