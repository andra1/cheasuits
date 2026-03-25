export default function ScoreBadge({ value, thresholds }) {
  const t = thresholds || { high: 60, mid: 30 };
  let bg, text;

  if (value >= t.high) {
    bg = 'bg-red-100 text-red-800';
  } else if (value >= t.mid) {
    bg = 'bg-yellow-100 text-yellow-800';
  } else {
    bg = 'bg-gray-100 text-gray-700';
  }

  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${bg}`}>
      {value}
    </span>
  );
}
