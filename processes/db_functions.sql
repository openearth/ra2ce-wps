-- Explain. I need to create a default priorities table. 
-- create table ra2ce_2_0.therm_expans_priorities as select id, geom, fid, therm_expans_gebeurtenis, therm_expans_kans, therm_expans_impact_klasse

--Explain: the query below shall be done again with add column for every hazard. now I only had an experiment with one hazard.
--alter table ra2ce_2_0.hazards_matrix  
--add Bodemdaling_kans_klasse INT;


-- Explain: create table temp for hazard layers of therm_expans_kans the query below can be used to create the temp table when we receive the new classes ranges
-- from the front-end/request.  In the request I will need the hazard_id and 
--Explain: create one hazard table for every hazard. columns kans, kans_klasse, amsheep_klasse

--create table ra2ce_2_0.therm_expans as select id, geom, fid, "Therm_expans_kans" , therm_expans_kans_klasse , "Therm_expans_AMSHEEP"  from ra2ce_2_0.hazards_matrix;
--alter table ra2ce_2_0.therm_expans 
--rename column "Therm_expans_kans" to kans;

alter table ra2ce_2_0.therm_expans 
rename column therm_expans_kans_klasse to kans_klasse;

alter table ra2ce_2_0.therm_expans 
rename column "Therm_expans_AMSHEEP" to amsheep_klasse;

--Explain: In similar way I will be updating the temp that have been created above with the new classes ranges. 
--update temp.therm_expans_1234 
/*update ra2ce_2_0."Voorbeeld-TH-BD" 
set therm_expans_kans_klasse = case 
									when therm_expans_kans between 20 and 9999 then 1
									when therm_expans_kans between 6 and 19 then 2
									when therm_expans_kans between 2 and 5 then 3
									when therm_expans_kans between 0.5 and 4 then 4
									when therm_expans_kans between -9999 and 0.4 then 5
									else therm_expans_kans 
end
WHERE   therm_expans_kans  between -9999 and 9999;
*/
--SELECT floor(random()*(100-0.1+1))+0.1;





/*steps to create a prepare a table as I need it.  
 * see below
 */
create table ra2ce_2_0.bosberm as select id, geom, fid, bosberm_kans, bosberm_amsheep  from ra2ce_2_0.hazards_matrix;

alter table ra2ce_2_0.bosberm 
add kans_klasse INT;

alter table ra2ce_2_0.bosberm 
rename column bosberm_kans to kans;

alter table ra2ce_2_0.bosberm 
rename column bosberm_amsheep to amsheep_klasse;

update ra2ce_2_0.bosberm 
set kans_klasse = case 
									when kans between 20 and 9999 then 1
									when kans between 6 and 19 then 2
									when kans between 2 and 5 then 3
									when kans between 0.5 and 4 then 4
									when kans between -9999 and 0.4 then 5
									else kans 
end
where kans between -9999 and 9999;
