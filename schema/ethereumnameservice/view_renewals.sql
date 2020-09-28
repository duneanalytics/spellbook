CREATE OR REPLACE VIEW ethereumnameservice.view_renewals AS
SELECT * FROM ethereumnameservice."ETHRegistrarController_1_evt_NameRenewed"
UNION 
SELECT * FROM ethereumnameservice."ETHRegistrarController_2_evt_NameRenewed"
UNION 
SELECT * FROM ethereumnameservice."ETHRegistrarController_3_evt_NameRenewed";
