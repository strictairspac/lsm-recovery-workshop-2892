from pydantic import BaseModel, Field


class DrillCreateInput(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    dataset_id: int = Field(gt=0)
    segment_limit: int = Field(default=8, ge=1, le=128)


class DrillGroupInput(BaseModel):
    group_id: str = Field(min_length=1, max_length=64)
    operations: list[dict] = Field(min_length=1, max_length=2000)
    crash: str = Field(default='none', pattern='^(none|before_fsync|after_fsync)$')


class DrillCheckpointInput(BaseModel):
    interrupt: bool = False


class DrillRecoveryInput(BaseModel):
    reason: str = Field(default='manual', max_length=40)
