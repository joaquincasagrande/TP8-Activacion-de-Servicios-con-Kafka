import { IsIn, IsNotEmpty, IsOptional, IsString } from 'class-validator';
import { SIMULATE_FAILURE_MODES, SimulateFailureMode } from '@app/contracts';

export class CreateActivationDto {
  @IsString()
  @IsNotEmpty()
  customerId: string;

  @IsString()
  @IsNotEmpty()
  planId: string;

  @IsString()
  @IsOptional()
  channel?: string = 'web';

  @IsOptional()
  @IsIn([
    SIMULATE_FAILURE_MODES.NONE,
    SIMULATE_FAILURE_MODES.BILLING,
    SIMULATE_FAILURE_MODES.PROVISIONING,
  ])
  simulateFailure?: SimulateFailureMode = SIMULATE_FAILURE_MODES.NONE;
}
