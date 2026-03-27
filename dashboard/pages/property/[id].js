import fs from 'fs';
import path from 'path';
import Head from 'next/head';
import Link from 'next/link';
import { useRouter } from 'next/router';
import ScoreBadge from '../../components/ScoreBadge';
import ScoreBar from '../../components/ScoreBar';

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

const CONFIDENCE_BADGE = {
  high: 'bg-green-100 text-green-800',
  medium: 'bg-yellow-100 text-yellow-800',
  low: 'bg-red-100 text-red-800',
};

const VIABILITY_BADGE = {
  high: 'bg-green-100 text-green-800',
  medium: 'bg-yellow-100 text-yellow-800',
  low: 'bg-red-100 text-red-800',
};

function viabilityTier(score) {
  if (score == null) return null;
  if (score >= 65) return 'high';
  if (score >= 35) return 'medium';
  return 'low';
}

function distressTier(score) {
  if (score == null) return null;
  if (score >= 60) return 'high';
  if (score >= 30) return 'medium';
  return 'low';
}

function Badge({ text, className }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${className}`}>
      {text}
    </span>
  );
}

function formatCurrency(v) {
  if (v == null) return '\u2014';
  return '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function Card({ title, children }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6">
      <h3 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-4">{title}</h3>
      {children}
    </div>
  );
}

function DetailRow({ label, value, className }) {
  if (value == null || value === '') return null;
  return (
    <div className="flex justify-between items-start py-1.5">
      <span className="text-sm text-gray-500">{label}</span>
      <span className={`text-sm text-right ${className || 'text-gray-900'}`}>{value}</span>
    </div>
  );
}

export async function getStaticPaths() {
  const dataPath = path.join(process.cwd(), 'public', 'data.json');
  const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
  const paths = data.features.map(f => ({
    params: { id: f.document_number }
  }));
  return { paths, fallback: false };
}

export async function getStaticProps({ params }) {
  const dataPath = path.join(process.cwd(), 'public', 'data.json');
  const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
  const property = data.features.find(f => f.document_number === params.id);
  if (!property) return { notFound: true };

  const features = data.features;
  const dates = features.map(f => f.recorded_date).filter(Boolean).sort();
  const earliest = dates[0] || '';
  const latest = dates[dates.length - 1] || '';
  const range = earliest && latest ? new Date(latest) - new Date(earliest) : 1;

  let score = 0;
  if (property.tax_status === 'sold') score += 40;
  else if (property.tax_status === 'delinquent') score += 25;
  if (property.case_type === 'FC') score += 20;
  else if (property.case_type === 'CH') score += 10;
  else if (property.case_type === 'CV') score += 5;
  if (property.absentee_owner) score += 15;
  if (property.recorded_date && range > 0) {
    const elapsed = new Date(property.recorded_date) - new Date(earliest);
    score += Math.round((elapsed / range) * 25);
  }

  return { props: { property: { ...property, score } } };
}

export default function PropertyDetail({ property }) {
  const router = useRouter();
  const f = property;

  const vTier = viabilityTier(f.viability_score);
  const dTier = distressTier(f.score);

  let viabilityDetails = null;
  if (f.viability_details) {
    try {
      viabilityDetails = JSON.parse(f.viability_details);
    } catch {
      // ignore parse errors
    }
  }

  const addressDisplay = f.property_address
    ? f.property_address.replace(/\n/g, ', ')
    : 'Unknown Address';

  function scoreBarColor(value, max) {
    const pct = max > 0 ? value / max : 0;
    if (pct >= 0.7) return 'bg-green-500';
    if (pct >= 0.4) return 'bg-yellow-500';
    return 'bg-red-400';
  }

  return (
    <>
      <Head>
        <title>{addressDisplay} — Property Detail</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>

      <div className="min-h-screen bg-gray-100">
        {/* Top Bar */}
        <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center gap-4 sticky top-0 z-20">
          <Link href="/" className="text-sm text-blue-600 hover:text-blue-800 font-medium flex items-center gap-1">
            <span>&larr;</span> Back to Table
          </Link>
          <span className="text-gray-300">|</span>
          <span className="text-sm text-gray-500">Property Detail</span>
        </div>

        <div className="max-w-7xl mx-auto px-6 py-6">
          {/* Header */}
          <div className="mb-6">
            <h1 className="text-2xl font-bold text-gray-900">{addressDisplay}</h1>
            <div className="flex flex-wrap items-center gap-3 mt-2">
              {f.owner_name && (
                <span className="text-gray-600">{f.owner_name}</span>
              )}
              {f.parcel_id && (
                <span className="text-sm text-gray-400">Parcel: {f.parcel_id}</span>
              )}
              {f.case_number && (
                <span className="flex items-center gap-1.5 text-sm text-gray-500">
                  Case: {f.case_number}
                  {f.case_type && (
                    <Badge
                      text={f.case_type}
                      className={CASE_BADGE[f.case_type] || 'bg-gray-100 text-gray-700'}
                    />
                  )}
                </span>
              )}
            </div>
          </div>

          {/* Score Summary Bar */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-6">
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-5 flex items-center gap-4">
              <div className="flex-1">
                <div className="text-xs text-gray-400 uppercase tracking-wider font-bold">Viability Score</div>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-3xl font-bold text-gray-900">{f.viability_score != null ? f.viability_score : '\u2014'}</span>
                  <span className="text-sm text-gray-400">/ 100</span>
                  {vTier && <Badge text={vTier} className={VIABILITY_BADGE[vTier]} />}
                </div>
                <div className="text-xs text-gray-500 mt-1">Investment readiness</div>
              </div>
            </div>
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-5 flex items-center gap-4">
              <div className="flex-1">
                <div className="text-xs text-gray-400 uppercase tracking-wider font-bold">Distress Score</div>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-3xl font-bold text-gray-900">{f.score != null ? f.score : '\u2014'}</span>
                  <span className="text-sm text-gray-400">/ 100</span>
                  {f.score != null && <ScoreBadge value={f.score} />}
                </div>
                <div className="text-xs text-gray-500 mt-1">Owner distress level</div>
              </div>
            </div>
          </div>

          {/* Card Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
            {/* Card 1: Financial Overview */}
            <Card title="Financial Overview">
              <div className="space-y-1">
                <div className="mb-3">
                  <div className="text-xs text-gray-500">Estimated Market Value</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {f.estimated_market_value != null ? formatCurrency(f.estimated_market_value) : '\u2014'}
                  </div>
                  {(() => {
                    if (!f.valuations || f.valuations.length === 0) return null;
                    const redfin = f.valuations.find(v => v.source === 'redfin');
                    const zillow = f.valuations.find(v => v.source === 'zillow');
                    const winner = redfin || zillow;
                    if (!winner) return null;
                    return (
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-xs text-gray-500 capitalize">Source: {winner.source}</span>
                        {winner.source_url && (
                          <a
                            href={winner.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-xs text-blue-600 hover:text-blue-800 underline"
                          >
                            View on {winner.source.charAt(0).toUpperCase() + winner.source.slice(1)} &uarr;
                          </a>
                        )}
                      </div>
                    );
                  })()}
                </div>
                <DetailRow label="Assessed Value" value={formatCurrency(f.assessed_value)} />
                {f.valuations && f.valuations.length > 0 && (
                  <div className="mt-4 pt-3 border-t border-gray-100">
                    <div className="text-xs text-gray-400 uppercase tracking-wider font-bold mb-2">All Valuations</div>
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left text-xs text-gray-400">
                          <th className="py-1">Source</th>
                          <th className="py-1">Estimate</th>
                          <th className="py-1">Confidence</th>
                          <th className="py-1"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {f.valuations.map((v, idx) => (
                          <tr key={idx} className="border-t border-gray-50">
                            <td className="py-1.5 capitalize">{v.source}{v.comp_count ? ` (${v.comp_count})` : ''}</td>
                            <td className="py-1.5">{formatCurrency(v.estimate)}</td>
                            <td className="py-1.5">
                              {v.confidence && (
                                <Badge
                                  text={v.confidence}
                                  className={CONFIDENCE_BADGE[v.confidence] || 'bg-gray-100 text-gray-700'}
                                />
                              )}
                            </td>
                            <td className="py-1.5">
                              {v.source_url && (
                                <a
                                  href={v.source_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-xs text-blue-600 hover:text-blue-800"
                                >
                                  View &uarr;
                                </a>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </Card>

            {/* Card 2: Equity & Liens */}
            <Card title="Equity &amp; Liens">
              <div className="space-y-1">
                {f.equity_spread != null && (
                  <div className="mb-3">
                    <div className="text-xs text-gray-500">Equity Spread</div>
                    <div className={`text-2xl font-bold ${f.equity_spread < 0 ? 'text-red-600' : 'text-green-700'}`}>
                      {f.equity_spread < 0 ? '-' : ''}{formatCurrency(Math.abs(f.equity_spread))}
                      {f.equity_ratio != null && (
                        <span className="text-sm font-normal opacity-75 ml-1">({Math.round(f.equity_ratio * 100)}%)</span>
                      )}
                    </div>
                  </div>
                )}
                {f.total_lien_burden != null && (
                  <DetailRow label="Total Lien Burden" value={formatCurrency(f.total_lien_burden)} className="font-semibold text-gray-900" />
                )}
                {f.mortgage_amount != null && f.mortgage_amount > 0 && (
                  <DetailRow label="Active Mortgage" value={formatCurrency(f.mortgage_amount)} />
                )}
                {f.total_mortgage_debt != null && f.total_mortgage_debt > 0 && f.mortgage_count > 1 && (
                  <DetailRow label="Total Mortgage Debt" value={`${formatCurrency(f.total_mortgage_debt)} (${f.mortgage_count} mortgages)`} />
                )}
                {f.mortgage_lender && (
                  <DetailRow label="Lender" value={f.mortgage_lender} />
                )}
                {f.mortgage_date && (
                  <DetailRow label="Mortgage Date" value={f.mortgage_date} />
                )}
                {f.federal_tax_lien_amount != null && f.federal_tax_lien_amount > 0 && (
                  <DetailRow label="Federal Tax Liens" value={formatCurrency(f.federal_tax_lien_amount)} />
                )}
                {f.state_tax_lien_amount != null && f.state_tax_lien_amount > 0 && (
                  <DetailRow label="State Tax Liens" value={formatCurrency(f.state_tax_lien_amount)} />
                )}
                {f.judgment_lien_amount != null && f.judgment_lien_amount > 0 && (
                  <DetailRow label="Judgment Liens" value={formatCurrency(f.judgment_lien_amount)} />
                )}
              </div>
            </Card>

            {/* Card 3: Viability Score Breakdown */}
            <Card title="Viability Score Breakdown">
              <div className="space-y-1">
                <div className="flex items-center gap-2 mb-4">
                  <span className="text-3xl font-bold text-gray-900">{f.viability_score != null ? f.viability_score : '\u2014'}</span>
                  <span className="text-sm text-gray-400">/ 100</span>
                  {vTier && <Badge text={vTier} className={VIABILITY_BADGE[vTier]} />}
                </div>
                {viabilityDetails ? (
                  <div className="space-y-3">
                    <ScoreBar label="Equity" value={viabilityDetails.equity_spread ?? 0} max={30} color={scoreBarColor(viabilityDetails.equity_spread ?? 0, 30)} />
                    <ScoreBar label="Comps" value={viabilityDetails.comp_confidence ?? 0} max={20} color={scoreBarColor(viabilityDetails.comp_confidence ?? 0, 20)} />
                    <ScoreBar label="Vacancy" value={viabilityDetails.neighborhood_vacancy ?? 0} max={15} color={scoreBarColor(viabilityDetails.neighborhood_vacancy ?? 0, 15)} />
                    <ScoreBar label="Buy Box" value={viabilityDetails.buy_box ?? 0} max={15} color={scoreBarColor(viabilityDetails.buy_box ?? 0, 15)} />
                    <ScoreBar label="Time Pressure" value={viabilityDetails.time_pressure ?? 0} max={10} color={scoreBarColor(viabilityDetails.time_pressure ?? 0, 10)} />
                    <ScoreBar label="Reachability" value={viabilityDetails.owner_reachability ?? 0} max={10} color={scoreBarColor(viabilityDetails.owner_reachability ?? 0, 10)} />
                  </div>
                ) : (
                  <p className="text-sm text-gray-400">No breakdown available</p>
                )}
              </div>
            </Card>

            {/* Card 4: Property Details */}
            <Card title="Property Details">
              <div className="space-y-1">
                <DetailRow label="Address" value={f.property_address ? f.property_address.replace(/\n/g, ', ') : '\u2014'} />
                <DetailRow label="Parcel ID" value={f.parcel_id} />
                <DetailRow label="Property Class" value={f.property_class} />
                <DetailRow label="Acres" value={f.acres != null ? f.acres : null} />
                <DetailRow label="Subdivision" value={f.subdivision} />
              </div>
            </Card>

            {/* Card 5: Owner & Contact */}
            <Card title="Owner &amp; Contact">
              <div className="space-y-1">
                <DetailRow label="Owner" value={f.owner_name} />
                <DetailRow
                  label="Mailing Address"
                  value={f.mailing_address ? f.mailing_address.replace(/\n/g, ', ') : null}
                />
                <div className="flex justify-between items-center py-1.5">
                  <span className="text-sm text-gray-500">Absentee Owner</span>
                  {f.absentee_owner ? (
                    <Badge text="Yes" className="bg-orange-100 text-orange-800" />
                  ) : (
                    <Badge text="No" className="bg-gray-100 text-gray-600" />
                  )}
                </div>
              </div>
            </Card>

            {/* Card 6: Case & Filing */}
            <Card title="Case &amp; Filing">
              <div className="space-y-1">
                <div className="flex justify-between items-center py-1.5">
                  <span className="text-sm text-gray-500">Case Number</span>
                  <span className="flex items-center gap-1.5 text-sm text-gray-900">
                    {f.case_number || '\u2014'}
                    {f.case_type && (
                      <Badge
                        text={`${f.case_type} \u2014 ${CASE_LABELS[f.case_type] || 'Other'}`}
                        className={CASE_BADGE[f.case_type] || 'bg-gray-100 text-gray-700'}
                      />
                    )}
                  </span>
                </div>
                <DetailRow label="Filed" value={f.recorded_date} />
                <DetailRow label="Document #" value={f.document_number} />
                <DetailRow label="Defendant" value={f.party2} />
              </div>
            </Card>

            {/* Card 7: Tax Information */}
            <Card title="Tax Information">
              <div className="space-y-1">
                <div className="flex justify-between items-center py-1.5">
                  <span className="text-sm text-gray-500">Tax Status</span>
                  {f.tax_status ? (
                    <Badge text={f.tax_status} className={TAX_BADGE[f.tax_status] || 'bg-gray-100 text-gray-700'} />
                  ) : (
                    <span className="text-sm text-gray-900">{'\u2014'}</span>
                  )}
                </div>
                <DetailRow label="Total Tax" value={f.total_tax != null ? formatCurrency(f.total_tax) : null} />
                <DetailRow label="Tax Rate" value={f.tax_rate != null ? f.tax_rate : null} />
                <DetailRow label="Net Taxable Value" value={f.net_taxable_value != null ? formatCurrency(f.net_taxable_value) : null} />
              </div>
            </Card>
          </div>
        </div>
      </div>
    </>
  );
}
