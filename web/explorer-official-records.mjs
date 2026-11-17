// Read-only view of existing Gold role assignments. No Explorer scoring or eligibility.
export function officialGoldRecords(rows){
 return rows.filter(row=>row.roles?.includes('recommended_alternative'))
  .sort((a,b)=>Date.parse(a.kickoff)-Date.parse(b.kickoff)||String(a.id).localeCompare(String(b.id)));
}
