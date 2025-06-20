CREATE PROCEDURE upsert_flights
	@flight_id NVARCHAR(20),
	@call_sign NVARCHAR(20),
	@origin_country NVARCHAR(50),
	@time_position BIGINT,
	@last_contact BIGINT,
	@longitude FLOAT,
	@latitude FLOAT,
	@altitude FLOAT,
	@on_ground INT,
	@velocity FLOAT
AS 
BEGIN
	IF EXISTS(SELECT 1 FROM flights_info WHERE flight_id = @flight_id)
		BEGIN
			UPDATE flights_info
			SET call_sign = @call_sign,
				origin_country = @origin_country,
				time_position = @time_position,
				last_contact = @last_contact,
				longitude = @longitude,
				latitude = @latitude,
				altitude = @altitude,
				on_ground = @on_ground,
				velocity = @velocity
			WHERE
				flight_id = @flight_id
		END
	ELSE
		BEGIN
			INSERT INTO flights_info(
				flight_id,
				call_sign,
				origin_country,
				time_position,
				last_contact,
				longitude,
				latitude,
				altitude,
				on_ground,
				velocity)
			VALUES(
				@flight_id,
				@call_sign,
				@origin_country,
				@time_position,
				@last_contact,
				@longitude,
				@latitude,
				@altitude,
				@on_ground,
				@velocity)
		END
END

CREATE PROCEDURE clean_flights
AS
BEGIN
	UPDATE flights_info
	SET call_sign = NULL WHERE call_sign LIKE '';
	UPDATE flights_info
	SET origin_country = NULL WHERE origin_country LIKE '';
END
