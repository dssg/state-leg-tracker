--states catalogue
INSERT INTO catalogs.states (state_id, state_abbreviation, state) values
(1,'AL','alabama'),
(2,'AK','alaska'),
(3,'AZ','arizona'),
(4,'AR','arkansas'),
(5,'CA','california'),
(6,'CO','colorado'),
(7,'CT','connecticut'),
(8,'DE','delaware'),
(9,'FL','florida'),
(10,'GA','georgia'),
(11,'HI','hawaii'),
(12,'ID','idaho'),
(13,'IL','illinois'),
(14,'IN','indiana'),
(15,'IA','iowa'),
(16,'KS','kansas'),
(17,'KY','kentucky'),
(18,'LA','louisiana'),
(19,'ME','maine'),
(20,'MD','maryland'),
(21,'MA','massachusetts'),
(22,'MI','michigan'),
(23,'MN','minnesota'),
(24,'MS','mississippi'),
(25,'MO','missouri'),
(26,'MT','montana'),
(27,'NE','nebraska'),
(28,'NV','nevada'),
(29,'NH','new hampshire'),
(30,'NJ','new jersey'),
(31,'NM','new mexico'),
(32,'NY','new york'),
(33,'NC','north carolina'),
(34,'ND','north dakota'),
(35,'OH','ohio'),
(36,'OK','oklahoma'),
(37,'OR','oregon'),
(38,'PA','pennsylvania'),
(39,'RI','rhode island'),
(40,'SC','south carolina'),
(41,'SD','south dakota'),
(42,'TN','tennessee'),
(43,'TX','texas'),
(44,'UT','utah'),
(45,'VT','vermont'),
(46,'VA','virginia'),
(47,'WA','washington'),
(48,'WV','west virginia'),
(49,'WI','wisconsin'),
(50,'WY','wyoming');

-- bill type catalog
insert into catalogs.bill_types (bill_type_id, bill_type, description) values
(1, 'B', 'Bill'),
(2, 'R', 'Resolution'),
(3, 'CR', 'Concurrent Resolution'),
(4, 'JR', 'Joint Resolution'),
(5, 'JRCA', 'Joint Resolution Constitutional Amendment'),
(6, 'EO', 'Executive Order'),
(7, 'CA', 'Constitutional Amendment'),
(8, 'M', 'Memorial'),
(9, 'CL', 'Claim'),
(10, 'C', 'Commendation'),
(11, 'CSR', 'Committee Study Request'),
(12, 'JM', 'Joint Memorial'),
(13, 'P', 'Proclamation'),
(14, 'SR', 'Study Request'),
(15, 'A', 'Address'),
(16, 'CM', 'Concurrent Memorial'),
(17, 'I', 'Initiative'),
(18, 'PET', 'Petition'),
(19, 'SB', 'Study Bill'),
(20, 'IP', 'Initiative Petition'),
(21, 'RB', 'Repeal Bill'),
(22, 'RM', 'Remonstration'),
(23, 'CB', 'Committee Bill');


-- Political party catalog
insert into catalogs.political_party (party_id, party_description) values
(1, 'Democrat'),
(2, 'Republican'),
(3, 'Independent'),
(4, 'Green Party'),
(5, 'Libertarian'),
(6, 'Nonpartisan');


-- Congress member roles
insert into catalogs.roles (role_id, role_name) values
(1, 'Representative / Lower Chamber'),
(2, 'Senator / Upper Chamber'),
(3, 'Joint Conference');


-- Bill sponsor types
insert into catalogs.sponsor_types (sponsor_type_id, sponsor_type) values
(0, 'Sponsor (Generic / Unspecified)'),
(1, 'Primary Sponsor'),
(2, 'Co-Sponsor'),
(3, 'Joint Sponsor');


-- Different statuses a bill goes through
insert into catalogs.bill_status (status_id, status, notes) values
(1, 'Introduced', ''),
(2, 'Engrossed', ''),
(3, 'Enrolled', ''),
(4, 'Passed', ''),
(5, 'Vetoed', ''),
(6, 'Failed', 'Limited support based on state'),
(7, 'Override', 'Progress array only'),
(8, 'Chaptered', 'Progress array only'),
(9, 'Refer', 'Progress array only'),
(10, 'Report Pass', 'Progress array only'),
(11, 'Report DNP', 'Progress array only'),
(12, 'Draft', 'Progress array only');


-- Supplemental documents 
insert into catalogs.supplement_types (supplement_type_id, supplement_type) values
(1, 'Fiscal Note'),
(2, 'Analysis'),
(3, 'Fiscal Note/Analysis'),
(4, 'Vote Image'),
(5, 'Local Mandate'),
(6, 'Corrections Impact'),
(7, 'Miscellaneous'),
(8, 'Veto Letter');


-- The types of bill texts
insert into catalogs.bill_text_types (text_type_id, text_type) values
-- Bill text types catalogue
INSERT INTO clean.bill_text_types (type_id, doc_type) values
(1, 'Introduced'),
(2, 'Committee Substitute'),
(3, 'Amended'),
(4, 'Engrossed'),
(5, 'Enrolled'),
(6, 'Chaptered'),
(7, 'Fiscal Note'),
(8, 'Analysis'),
(9, 'Draft'),
(10, 'Conference Substitute'),
(11, 'Prefiled'),
(12, 'Veto Message'),
(13, 'Veto Response'),
(14, 'Substitute');


-- Vote types
insert into catalogs.vote_types (vote_type_id, vote_type) values
(1, 'Yea'),
(2, 'Nay'),
(3, 'Not Voting / Abstain'),
(4, 'Absent / Excused');


-- US states catalog
INSERT INTO catalogs.states (state_id, state_abbreviation, state) values
(1,'AL','alabama'),
(2,'AK','alaska'),
(3,'AZ','arizona'),
(4,'AR','arkansas'),
(5,'CA','california'),
(6,'CO','colorado'),
(7,'CT','connecticut'),
(8,'DE','delaware'),
(9,'FL','florida'),
(10,'GA','georgia'),
(11,'HI','hawaii'),
(12,'ID','idaho'),
(13,'IL','illinois'),
(14,'IN','indiana'),
(15,'IA','iowa'),
(16,'KS','kansas'),
(17,'KY','kentucky'),
(18,'LA','louisiana'),
(19,'ME','main'),
(20,'MD','maryland'),
(21,'MA','massachusetss'),
(22,'MI','michigan'),
(23,'MN','minnesota'),
(24,'MS','mississippi'),
(25,'MO','missouri'),
(26,'MT','montana'),
(27,'NE','nebraska'),
(28,'NV','nevada'),
(29,'NH','new hampshire'),
(30,'NJ','new jersey'),
(31,'NM','new mexico'),
(32,'NY','new york'),
(33,'NC','north carolina'),
(34,'ND','north dakota'),
(35,'OH','ohio'),
(36,'OK','oklahoma'),
(37,'OR','oregon'),
(38,'PA','pennsylvania'),
(39,'RI','rhode island'),
(40,'SC','south carolina'),
(41,'SD','south dakota'),
(42,'TN','tennessee'),
(43,'TX','texas'),
(44,'UT','utah'),
(45,'VT','vermont'),
(46,'VA','virginia'),
(47,'WA','washington'),
(48,'WV','west virginia'),
(49,'WI','wisconsin'),
(50,'WY','wyoming');
