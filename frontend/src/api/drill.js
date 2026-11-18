import{api,post}from'./client.js';
export const list=()=>api('/drills');
export const create=data=>post('/drills',data);
export const detail=id=>api('/drills/'+id);
export const writeGroup=(id,data)=>post(`/drills/${id}/groups`,data);
export const checkpoint=(id,interrupt)=>post(`/drills/${id}/checkpoint`,{interrupt:Boolean(interrupt)});
export const crash=id=>post(`/drills/${id}/crash`,{});
export const recover=id=>post(`/drills/${id}/recover`,{reason:'manual'});
export async function download(id){const d=await detail(id);const url=URL.createObjectURL(new Blob([JSON.stringify(d,null,2)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download='durability-drill-'+id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)}
