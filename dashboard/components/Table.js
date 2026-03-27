import { useState } from 'react';
import { useRouter } from 'next/router';
import ScoreBadge from './ScoreBadge';

const TAX_BADGE = {
  sold: 'bg-red-100 text-red-800',
  delinquent: 'bg-yellow-100 text-yellow-800',
  paid: 'bg-green-100 text-green-800',
};

const CASE_BADGE = {
  FC: 'bg-red-100 text-red-800',
  CH: 'bg-yellow-100 text-yellow-800',
  CV: 'bg-blue-100 text-blue-800',
};

const CONFIDENCE_BADGE = {
  high: 'bg-green-100 text-green-800',
  medium: 'bg-yellow-100 text-yellow-800',
  low: 'bg-red-100 text-red-800',
};

const VIABILITY_BADGE = {
  high:   'bg-green-100 text-green-800',
  medium: 'bg-yellow-100 text-yellow-800',
  low:    'bg-red-100 text-red-800',
};

function viabilityTier(score) {
  if (score == null) return null;
  if (score >= 65) return 'high';
  if (score >= 35) return 'medium';
  return 'low';
}

const COLUMNS = [
  { key: 'viability_score', label: 'Viability' },
  { key: 'score', label: 'Distress' },
  { key: 'case_number', label: 'Case #' },
  { key: 'recorded_date', label: 'Filed' },
  { key: 'owner_name', label: 'Owner' },
  { key: 'property_address', label: 'Address' },
  { key: 'estimated_market_value', label: 'Est. Value' },
  { key: 'equity_spread', label: 'Equity Spread' },
  { key: 'tax_status', label: 'Tax Status' },
  { key: 'absentee_owner', label: 'Absentee' },
];

function Badge({ text, className }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${className}`}>
      {text}
    </span>
  );
}

function SortArrow({ column, sortKey, sortDir }) {
  if (column !== sortKey) return <span className="text-gray-500 ml-1">&#8597;</span>;
  return <span className="ml-1">{sortDir === 'asc' ? '\u25B2' : '\u25BC'}</span>;
}

function formatCurrency(v) {
  if (v == null) return '\u2014';
  return '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export default function Table({ features }) {
  const router = useRouter();
  const [sortKey, setSortKey] = useState('viability_score');
  const [sortDir, setSortDir] = useState('desc');

  function handleSort(key) {
    if (key === sortKey) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir(key === 'score' || key === 'viability_score' || key === 'estimated_market_value' || key === 'equity_spread' ? 'desc' : 'asc');
    }
  }

  const sorted = [...features].sort((a, b) => {
    let av = a[sortKey];
    let bv = b[sortKey];
    if (av == null) av = '';
    if (bv == null) bv = '';
    if (typeof av === 'number' && typeof bv === 'number') {
      return sortDir === 'asc' ? av - bv : bv - av;
    }
    if (typeof av === 'boolean') {
      return sortDir === 'asc' ? (av === bv ? 0 : av ? 1 : -1) : (av === bv ? 0 : av ? -1 : 1);
    }
    const cmp = String(av).localeCompare(String(bv));
    return sortDir === 'asc' ? cmp : -cmp;
  });

  function renderCell(f, col) {
    switch (col.key) {
      case 'viability_score': {
        if (f.viability_score == null) return '\u2014';
        const tier = viabilityTier(f.viability_score);
        return (
          <span className="flex items-center gap-1.5">
            <span className="font-bold tabular-nums">{f.viability_score}</span>
            <Badge text={tier} className={VIABILITY_BADGE[tier]} />
          </span>
        );
      }
      case 'score':
        return <ScoreBadge value={f.score} />;
      case 'case_number':
        return (
          <span className="flex items-center gap-1.5">
            {f.case_number}
            {f.case_type && (
              <Badge
                text={f.case_type}
                className={CASE_BADGE[f.case_type] || 'bg-gray-100 text-gray-700'}
              />
            )}
          </span>
        );
      case 'owner_name':
        return f.owner_name || f.party2 || '\u2014';
      case 'property_address':
        return f.property_address ? f.property_address.replace(/\n/g, ', ') : '\u2014';
      case 'estimated_market_value':
        return f.estimated_market_value != null ? (
          <span className="flex items-center gap-1.5">
            {formatCurrency(f.estimated_market_value)}
            {f.valuation_confidence && (
              <Badge
                text={f.valuation_confidence}
                className={CONFIDENCE_BADGE[f.valuation_confidence] || 'bg-gray-100 text-gray-700'}
              />
            )}
          </span>
        ) : '\u2014';
      case 'equity_spread': {
        if (f.equity_spread == null) return '\u2014';
        const isNeg = f.equity_spread < 0;
        const ratio = f.equity_ratio != null ? ` (${Math.round(f.equity_ratio * 100)}%)` : '';
        return (
          <span className={isNeg ? 'text-red-600 font-medium' : 'text-green-700 font-medium'}>
            {isNeg ? '-' : ''}{formatCurrency(Math.abs(f.equity_spread))}
            <span className="text-xs opacity-75">{ratio}</span>
          </span>
        );
      }
      case 'tax_status':
        return f.tax_status ? (
          <Badge
            text={f.tax_status}
            className={TAX_BADGE[f.tax_status] || 'bg-gray-100 text-gray-700'}
          />
        ) : '\u2014';
      case 'absentee_owner':
        return f.absentee_owner ? (
          <Badge text="Yes" className="bg-orange-100 text-orange-800" />
        ) : (
          <Badge text="No" className="bg-gray-100 text-gray-600" />
        );
      default:
        return f[col.key] ?? '\u2014';
    }
  }

  return (
    <div className="h-full overflow-auto">
      <table className="w-full text-sm text-left">
        <thead className="sticky top-0 bg-gray-100 text-gray-700 uppercase text-xs z-10">
          <tr>
            {COLUMNS.map((col) => (
              <th
                key={col.key}
                className="px-3 py-2 cursor-pointer select-none whitespace-nowrap hover:bg-gray-200"
                onClick={() => handleSort(col.key)}
              >
                {col.label}
                <SortArrow column={col.key} sortKey={sortKey} sortDir={sortDir} />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((f, i) => (
            <tr
              key={f.document_number}
              className={`cursor-pointer transition-colors ${
                i % 2 === 0
                  ? 'bg-white hover:bg-gray-100'
                  : 'bg-gray-50 hover:bg-gray-100'
              }`}
              onClick={() => router.push(`/property/${f.document_number}`)}
            >
              {COLUMNS.map((col) => (
                <td key={col.key} className="px-3 py-2 whitespace-nowrap">
                  {renderCell(f, col)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
