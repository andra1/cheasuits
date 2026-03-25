import { useState } from 'react';
import fs from 'fs';
import path from 'path';
import Head from 'next/head';
import dynamic from 'next/dynamic';
import Sidebar from '../components/Sidebar';
import Table from '../components/Table';

const Map = dynamic(() => import('../components/Map'), { ssr: false });

export async function getStaticProps() {
  const dataPath = path.join(process.cwd(), 'public', 'data.json');

  let data = {
    generated_at: '',
    total_records: 0,
    geocoded_count: 0,
    date_range: { earliest: '', latest: '' },
    summary: { total: 0 },
    features: [],
  };

  if (fs.existsSync(dataPath)) {
    const raw = fs.readFileSync(dataPath, 'utf-8');
    data = JSON.parse(raw);
  }

  return { props: { data } };
}

function computeScores(features) {
  const dates = features
    .map((f) => f.recorded_date)
    .filter(Boolean)
    .sort();
  const earliest = dates[0] || '';
  const latest = dates[dates.length - 1] || '';
  const range = earliest && latest ? new Date(latest) - new Date(earliest) : 1;

  return features.map((f) => {
    let score = 0;

    // Tax status: sold=40, delinquent=25, paid=0
    if (f.tax_status === 'sold') score += 40;
    else if (f.tax_status === 'delinquent') score += 25;

    // Case type: FC=20, CH=10, CV=5
    if (f.case_type === 'FC') score += 20;
    else if (f.case_type === 'CH') score += 10;
    else if (f.case_type === 'CV') score += 5;

    // Absentee owner: 15
    if (f.absentee_owner) score += 15;

    // Recency: linear 0–25 based on recorded_date
    if (f.recorded_date && range > 0) {
      const elapsed = new Date(f.recorded_date) - new Date(earliest);
      score += Math.round((elapsed / range) * 25);
    }

    return { ...f, score };
  });
}

export default function Home({ data }) {
  const [tab, setTab] = useState('map');
  const scored = computeScores(data.features);
  const mappable = scored.filter((f) => f.lat != null && f.lng != null);

  return (
    <>
      <Head>
        <title>Lis Pendens Dashboard — St. Clair County</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>
      <div className="flex h-screen">
        <Sidebar data={data} />
        <div className="flex-1 flex flex-col">
          <div className="flex gap-1 p-2 bg-gray-100 border-b border-gray-200">
            <button
              onClick={() => setTab('map')}
              className={`px-4 py-1.5 rounded text-sm font-medium ${
                tab === 'map'
                  ? 'bg-white shadow text-gray-900'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              Map
            </button>
            <button
              onClick={() => setTab('table')}
              className={`px-4 py-1.5 rounded text-sm font-medium ${
                tab === 'table'
                  ? 'bg-white shadow text-gray-900'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              Table
            </button>
          </div>
          <div className="flex-1 relative overflow-hidden">
            {tab === 'map' ? <Map features={mappable} /> : <Table features={scored} />}
          </div>
        </div>
      </div>
    </>
  );
}
