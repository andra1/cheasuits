import { Fragment, useState } from 'react';
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

const CASE_LABELS = {
  FC: 'Foreclosure',
  CH: 'Chancery',
  CV: 'Civil',
};

const COLUMNS = [
  { key: 'score', label: 'Score' },
  { key: 'case_number', label: 'Case #' },
  { key: 'recorded_date', label: 'Filed' },
  { key: 'owner_name', label: 'Owner' },
  { key: 'property_address', label: 'Address' },
  { key: 'tax_status', label: 'Tax Status' },
  { key: 'absentee_owner', label: 'Absentee' },
  { key: 'property_class', label: 'Class' },
  { key: 'acres', label: 'Acres' },
  { key: 'parcel_id', label: 'Parcel' },
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
  return <span className="ml-1">{sortDir === 'asc' ? '▲' : '▼'}</span>;
}

function DetailField({ label, value }) {
  if (value == null || value === '') return null;
  return (
    <div>
      <dt className="text-xs text-gray-500 uppercase tracking-wide">{label}</dt>
      <dd className="text-sm text-gray-900 mt-0.5">{value}</dd>
    </div>
  );
}

function formatCurrency(v) {
  if (v == null) return '—';
  return '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function DetailPanel({ f }) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-4 p-5 bg-gray-50 border-t border-b border-gray-200">
      {/* Case Info */}
      <div className="space-y-3">
        <h4 className="text-xs font-bold text-gray-400 uppercase tracking-wider">Case Info</h4>
        <DetailField label="Case Number" value={f.case_number} />
        <DetailField
          label="Case Type"
          value={f.case_type ? `${f.case_type} — ${CASE_LABELS[f.case_type] || 'Other'}` : null}
        />
        <DetailField label="Filed" value={f.recorded_date} />
        <DetailField label="Document #" value={f.document_number} />
        <DetailField label="Defendant" value={f.party2} />
      </div>

      {/* Owner & Mailing */}
      <div className="space-y-3">
        <h4 className="text-xs font-bold text-gray-400 uppercase tracking-wider">Owner &amp; Mailing</h4>
        <DetailField label="Owner" value={f.owner_name} />
        <DetailField
          label="Property Address"
          value={f.property_address ? f.property_address.replace(/\n/g, ', ') : null}
        />
        <DetailField
          label="Mailing Address"
          value={f.mailing_address ? f.mailing_address.replace(/\n/g, ', ') : null}
        />
        <div>
          <dt className="text-xs text-gray-500 uppercase tracking-wide">Absentee Owner</dt>
          <dd className="mt-0.5">
            {f.absentee_owner ? (
              <Badge text="Yes" className="bg-orange-100 text-orange-800" />
            ) : (
              <Badge text="No" className="bg-gray-100 text-gray-600" />
            )}
          </dd>
        </div>
      </div>

      {/* Valuation & Tax */}
      <div className="space-y-3">
        <h4 className="text-xs font-bold text-gray-400 uppercase tracking-wider">Valuation &amp; Tax</h4>
        <DetailField label="Assessed Value" value={formatCurrency(f.assessed_value)} />
        <DetailField label="Net Taxable Value" value={formatCurrency(f.net_taxable_value)} />
        <DetailField label="Tax Rate" value={f.tax_rate != null ? f.tax_rate : null} />
        <DetailField label="Total Tax" value={f.total_tax != null ? formatCurrency(f.total_tax) : null} />
        <div>
          <dt className="text-xs text-gray-500 uppercase tracking-wide">Tax Status</dt>
          <dd className="mt-0.5">
            {f.tax_status ? (
              <Badge text={f.tax_status} className={TAX_BADGE[f.tax_status] || 'bg-gray-100 text-gray-700'} />
            ) : '—'}
          </dd>
        </div>
      </div>

      {/* Parcel Details */}
      <div className="space-y-3">
        <h4 className="text-xs font-bold text-gray-400 uppercase tracking-wider">Parcel Details</h4>
        <DetailField label="Parcel ID" value={f.parcel_id} />
        <DetailField label="Property Class" value={f.property_class} />
        <DetailField label="Acres" value={f.acres != null ? f.acres : null} />
        <DetailField label="Subdivision" value={f.subdivision} />
        <div>
          <dt className="text-xs text-gray-500 uppercase tracking-wide">Distress Score</dt>
          <dd className="mt-0.5"><ScoreBadge value={f.score} /> <span className="text-xs text-gray-400">/ 100</span></dd>
        </div>
      </div>
    </div>
  );
}

export default function Table({ features }) {
  const [sortKey, setSortKey] = useState('score');
  const [sortDir, setSortDir] = useState('desc');
  const [expandedId, setExpandedId] = useState(null);

  function handleSort(key) {
    if (key === sortKey) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir(key === 'score' ? 'desc' : 'asc');
    }
  }

  function toggleRow(docNum) {
    setExpandedId(expandedId === docNum ? null : docNum);
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
        return f.owner_name || f.party2 || '—';
      case 'property_address':
        return f.property_address ? f.property_address.replace(/\n/g, ', ') : '—';
      case 'tax_status':
        return f.tax_status ? (
          <Badge
            text={f.tax_status}
            className={TAX_BADGE[f.tax_status] || 'bg-gray-100 text-gray-700'}
          />
        ) : '—';
      case 'absentee_owner':
        return f.absentee_owner ? (
          <Badge text="Yes" className="bg-orange-100 text-orange-800" />
        ) : (
          <Badge text="No" className="bg-gray-100 text-gray-600" />
        );
      case 'acres':
        return f.acres != null ? f.acres : '—';
      default:
        return f[col.key] ?? '—';
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
          {sorted.map((f, i) => {
            const isExpanded = expandedId === f.document_number;
            return (
              <Fragment key={f.document_number}>
                <tr
                  className={`cursor-pointer transition-colors ${
                    isExpanded
                      ? 'bg-blue-50'
                      : i % 2 === 0
                        ? 'bg-white hover:bg-gray-100'
                        : 'bg-gray-50 hover:bg-gray-100'
                  }`}
                  onClick={() => toggleRow(f.document_number)}
                >
                  {COLUMNS.map((col) => (
                    <td key={col.key} className="px-3 py-2 whitespace-nowrap">
                      {renderCell(f, col)}
                    </td>
                  ))}
                </tr>
                {isExpanded && (
                  <tr>
                    <td colSpan={COLUMNS.length} className="p-0">
                      <DetailPanel f={f} />
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
