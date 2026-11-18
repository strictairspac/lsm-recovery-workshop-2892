/** @typedef {{id:number,name:string,description:string,entries:Array}} Dataset */
/** @typedef {{id:number,name:string,memtable_limit:number,l0_trigger:number,max_levels:number,final_compaction:boolean}} Config */
/** @typedef {{id:number,name:string,dataset_id:number,config_id:number,status:string,result:Object}} Trial */
/** @typedef {{id:number,name:string,dataset_id:number,status:string,checkpoint_lsn:number,last_lsn:number,group_count:number,last_report:Object}} Drill */
export const entityNames={dataset:'键值批次',config:'存储配置',trial:'合并试验',drill:'持久化演练'};
