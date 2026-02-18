CREATE DATABASE IF NOT EXISTS omis_reporting;
USE omis_reporting;
-- Table: headcount_growth
-- Description: Records the total headcount and basic hires/departures for each reporting period.
CREATE TABLE  IF NOT EXISTS headcount_growth (
  id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the record',
  period_date DATE NOT NULL COMMENT 'Date representing the reporting period (e.g., month-end)',
  total_headcount INT NOT NULL COMMENT 'Total number of employees at period end',
  new_hires INT NOT NULL COMMENT 'Number of employees hired during the period',
  new_departures INT NOT NULL COMMENT 'Number of employees who left during the period',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was created',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was last updated'
);

-- Table: demographic_gender
-- Description: Breaks down headcount by gender and age group for each reporting period.
CREATE TABLE IF NOT EXISTS demographic_gender (
  id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the record',
   age_18_24_f INT NOT NULL DEFAULT 0,
   age_25_29_f INT NOT NULL DEFAULT 0,
   age_30_44_f INT NOT NULL DEFAULT 0,
   age_45_60_f INT NOT NULL DEFAULT 0,
   age_60_plus_f INT NOT NULL DEFAULT 0,
   age_18_24_m INT NOT NULL DEFAULT 0,
   age_25_29_m INT NOT NULL DEFAULT 0,
   age_30_44_m INT NOT NULL DEFAULT 0,
   age_45_60_m INT NOT NULL DEFAULT 0,
   age_60_plus_m INT NOT NULL DEFAULT 0,
   age_60_plus_m INT NOT NULL DEFAULT 0,
   total_f INT NOT NULL DEFAULT 0,
   total_m INT NOT NULL DEFAULT 0,
   under_18_f INT NOT NULL DEFAULT 0,
   under_18_m INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was created',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was last updated'
);

-- Table: demographic_age
-- Description: Breaks down headcount by age ranges for each reporting period.
CREATE TABLE IF NOT EXISTS demographic_age (
  id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the record',
  period_date DATE NOT NULL COMMENT 'Date representing the reporting period',
  age_range VARCHAR(20) NOT NULL COMMENT 'Age range bucket (e.g., \u201818-25\u2019) <-- keys in demographic_age hash',
  count INT NOT NULL COMMENT 'Number of employees in this age range <-- values in demographic_age hash',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was created',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was last updated'
);

-- Table: demographic_tenure
-- Description: Breaks down headcount by tenure bands for each reporting period.
CREATE TABLE IF NOT EXISTS demographic_tenure (
  id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the record',
  period_date DATE NOT NULL COMMENT 'Date representing the reporting period',
  period_label VARCHAR(20) COMMENT 'name for the quarter',
  retention_rate VARCHAR(20) NOT NULL,
  turnover_rate VARCHAR(20) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was created',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was last updated'
);

-- Table: projects
-- Description: Records fundamental project details for tracking and reporting.
-- From a json "projects": {i}: {}
CREATE TABLE IF NOT EXISTS projects (
  id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the project',
  project_name VARCHAR(255) NOT NULL COMMENT 'Name or title of the project',
  project_start_date DATE NOT NULL COMMENT 'Planned or actual project start date',
  project_end_date DATE COMMENT 'Planned or actual project end date',
  project_status BOOLEAN NOT NULL DEFAULT true COMMENT 'Current status of the project',
  period_date DATE NOT NULL COMMENT 'Date corresponding to the project snapshot or update',
  project_workforce INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was created',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was last updated'
);

-- seed some data in all em