CREATE OR REPLACE VIEW ethereumnameservice.view_registrations AS
SELECT * FROM ethereumnameservice."ETHRegistrarController_1_evt_NameRegistered"
UNION 
SELECT * FROM ethereumnameservice."ETHRegistrarController_2_evt_NameRegistered"
UNION 
SELECT * FROM ethereumnameservice."ETHRegistrarController_3_evt_NameRegistered"
