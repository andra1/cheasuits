import { useState } from 'react';

const CONFIDENCE_BADGE = {
  high: 'bg-green-100 text-green-800',
  medium: 'bg-yellow-100 text-yellow-800',
  low: 'bg-red-100 text-red-800',
};

function Badge({ text, className }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${className}`}>
      {text}
    </span>
  );
}

function formatCurrency(v) {
  if (v == null) return '\u2014';
  return '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function CompSourceLink({ comp }) {
  if (comp.source === 'redfin' && comp.source_id) {
    return (
      <a
        href={`https://www.redfin.com/`}
        target="_blank"
        rel="noopener noreferrer"
        className="text-xs text-blue-600 hover:text-blue-800 underline"
      >
        View on Redfin &uarr;
      </a>
    );
  }
  if (comp.source === 'recorder') {
    const parcelMatch = comp.address.match(/Parcel\s+(\S+)/);
    if (parcelMatch) {
      const parcelId = parcelMatch[1].replace(/-/g, '');
      return (
        <a
          href={`https://stclairil.devnetwedge.com/parcel/view/${parcelId}`}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-blue-600 hover:text-blue-800 underline"
        >
          County Recorder &uarr;
        </a>
      );
    }
  }
  return null;
}

export default function CompsCard({ comps, valuations }) {
  const [expanded, setExpanded] = useState(null);

  const compVal = valuations?.find(v => v.source === 'comps');

  if (!comps || comps.length === 0) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
        <h3 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-4">Comparable Sales</h3>
        <p className="text-sm text-gray-400">No comparable sales found for this property.</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
      <h3 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-4">Comparable Sales</h3>

      {compVal && (
        <div className="flex gap-4 mb-4">
          <div className="bg-gray-50 rounded-lg px-4 py-2 text-center">
            <div className="text-xs text-gray-500">Comps Estimate</div>
            <div className="text-lg font-bold">{formatCurrency(compVal.estimate)}</div>
          </div>
          <div className="bg-gray-50 rounded-lg px-4 py-2 text-center">
            <div className="text-xs text-gray-500">Confidence</div>
            <div className="mt-1">
              {compVal.confidence && (
                <Badge text={compVal.confidence} className={CONFIDENCE_BADGE[compVal.confidence] || 'bg-gray-100 text-gray-700'} />
              )}
            </div>
          </div>
          <div className="bg-gray-50 rounded-lg px-4 py-2 text-center">
            <div className="text-xs text-gray-500">Comp Count</div>
            <div className="text-lg font-bold">{comps.length}</div>
          </div>
        </div>
      )}

      <div className="space-y-2">
        {comps.map((comp, idx) => {
          const isExpanded = expanded === idx;
          return (
            <div key={idx} className={`border rounded-lg overflow-hidden ${isExpanded ? 'border-blue-300' : 'border-gray-200'}`}>
              <button
                className="w-full px-4 py-3 flex items-center justify-between text-left bg-gray-50 hover:bg-gray-100"
                onClick={() => setExpanded(isExpanded ? null : idx)}
              >
                <div className="flex items-center gap-4 text-sm">
                  <span className="text-xs font-bold text-yellow-600">#{idx + 1}</span>
                  <span className="text-gray-900">{comp.address}</span>
                  <span className="font-semibold text-green-700">{formatCurrency(comp.sale_price)}</span>
                  <span className="text-gray-400 text-xs">
                    {comp.distance_miles != null ? `${comp.distance_miles.toFixed(1)} mi` : ''}
                    {comp.sale_date ? ` \u00B7 sold ${comp.sale_date}` : ''}
                  </span>
                </div>
                <span className="text-gray-400 text-xs">{isExpanded ? '\u25B2' : '\u25BC'}</span>
              </button>

              {isExpanded && (
                <div className="px-4 py-3 border-t border-gray-100 bg-white">
                  <div className="grid grid-cols-3 gap-3 text-sm mb-3">
                    <div>
                      <div className="text-xs text-gray-400">Sale Price</div>
                      <div>{formatCurrency(comp.sale_price)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Adjusted Price</div>
                      <div>{formatCurrency(comp.adjusted_price)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Distance</div>
                      <div>{comp.distance_miles != null ? `${comp.distance_miles.toFixed(2)} mi` : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Similarity</div>
                      <div>{comp.similarity_score != null ? comp.similarity_score.toFixed(2) : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Lot Size</div>
                      <div>{comp.lot_size != null ? `${comp.lot_size} ac` : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Lot Ratio</div>
                      <div>{comp.lot_size_ratio != null ? `${comp.lot_size_ratio.toFixed(2)}x` : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Beds / Baths</div>
                      <div>{comp.beds ?? '\u2014'} / {comp.baths ?? '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Sqft</div>
                      <div>{comp.sqft != null ? comp.sqft.toLocaleString() : '\u2014'}</div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-400">Year Built</div>
                      <div>{comp.year_built ?? '\u2014'}</div>
                    </div>
                  </div>
                  <div className="flex gap-3">
                    <CompSourceLink comp={comp} />
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
