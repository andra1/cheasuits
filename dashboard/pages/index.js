import fs from 'fs';
import path from 'path';
import Head from 'next/head';
import dynamic from 'next/dynamic';
import Sidebar from '../components/Sidebar';

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

export default function Home({ data }) {
  const mappable = data.features.filter((f) => f.lat != null && f.lng != null);

  return (
    <>
      <Head>
        <title>Lis Pendens Map — St. Clair County</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>
      <div className="flex h-screen">
        <Sidebar data={data} />
        <div className="flex-1 relative">
          <Map features={mappable} />
        </div>
      </div>
    </>
  );
}
