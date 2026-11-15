DROP TRIGGER observation_append_only_delete ON price_availability_observation;
DROP FUNCTION observation_no_delete();
DROP TRIGGER observation_append_only_update ON price_availability_observation;
DROP FUNCTION observation_append_only();
DROP TRIGGER observation_ordering_assign ON price_availability_observation;
DROP FUNCTION assign_observation_ordering();
ALTER TABLE price_availability_observation
  DROP CONSTRAINT observation_offer_position,
  DROP CONSTRAINT observation_payment_eligibility,
  DROP CONSTRAINT observation_price_status_consistency,
  DROP CONSTRAINT observation_price_status,
  DROP CONSTRAINT observation_position_positive,
  DROP COLUMN payment_eligible,
  DROP COLUMN price_status,
  DROP COLUMN position;
