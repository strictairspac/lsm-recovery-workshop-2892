import json

from fastapi import APIRouter, Depends, Response

from ..config.database import get_db
from ..schemas.drill import (
    DrillCheckpointInput,
    DrillCreateInput,
    DrillGroupInput,
    DrillRecoveryInput,
)
from ..services.auth import current_user, writer
from ..services.drill import checkpoint, crash, create, detail, export, listing, recover, status, write_group

router = APIRouter(prefix='/drills')


@router.get('')
def get(db=Depends(get_db), user=Depends(current_user)):
    return listing(db)


@router.post('', status_code=201)
def add(data: DrillCreateInput, db=Depends(get_db), user=Depends(writer)):
    return create(db, data)


@router.get('/{ident}')
def one(ident: int, db=Depends(get_db), user=Depends(current_user)):
    return detail(db, ident)


@router.get('/{ident}/status')
def drill_status(ident: int, db=Depends(get_db), user=Depends(current_user)):
    return status(db, ident)


@router.post('/{ident}/groups', status_code=201)
def put_group(ident: int, data: DrillGroupInput, db=Depends(get_db), user=Depends(writer)):
    return write_group(db, ident, data)


@router.post('/{ident}/checkpoint', status_code=201)
def make_checkpoint(ident: int, data: DrillCheckpointInput | None = None, db=Depends(get_db), user=Depends(writer)):
    return checkpoint(db, ident, interrupt=bool(data and data.interrupt))


@router.post('/{ident}/crash', status_code=201)
def simulate_power_loss(ident: int, db=Depends(get_db), user=Depends(writer)):
    return crash(db, ident)


@router.post('/{ident}/recover', status_code=201)
def run_recovery(ident: int, data: DrillRecoveryInput | None = None, db=Depends(get_db), user=Depends(writer)):
    return recover(db, ident, (data.reason if data else 'manual'))


@router.get('/{ident}/export')
def export_drill(ident: int, db=Depends(get_db), user=Depends(current_user)):
    payload = export(db, ident)
    return Response(
        json.dumps(payload, ensure_ascii=False, indent=2),
        media_type='application/json',
        headers={'Content-Disposition': f'attachment; filename="durability-drill-{ident}.json"'},
    )
