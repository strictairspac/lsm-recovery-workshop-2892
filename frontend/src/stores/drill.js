import{list}from'../api/drill.js';export const drills={items:[],async load(){this.items=await list();return this.items}};
