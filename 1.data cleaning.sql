-- Data Cleaning 

SELECT *
FROM layoffs;

-- Steps for Data Cleaning:
-- 1. Remove Duplicates 
-- 2. Standardize Data 
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns 

-- To duplicate tables without working on raw table 
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging 
SELECT *
FROM layoffs;

-- 1. Remove Duplicates 

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
DELETE 
FROM duplicate_cte 
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
	`company` text,
    `location` text, 
    `industry` text, 
    `total_laid_off` text, 
    `percentage_laid_off` text, 
    `date` text,
    `stage` text, 
    `country` text, 
    `funds_raised_millions` text,
    `row_num` INT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE *
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- Standardizing Data 

SELECT DISTINCT company
FROM layoffs_staging2;

SELECT DISTINCT(TRIM(company))
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT *
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging2; 
-- Y must be capped 

UPDATE layoffs_staging2
SET `date` = 
CASE
	WHEN `date` IS NOT NULL AND `date` <> 'None' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
    ELSE NULL
END;

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- REMOVE NULL/BLANK 

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off = 'None'
AND percentage_laid_off = 'None';

SELECT *
FROM layoffs_staging2
WHERE industry = 'None' 
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location 
WHERE (t1.industry = 'None' OR t1.industry = '')
AND t2.industry <> 'None';

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = ''; 

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location 
WHERE (t1.industry = 'Null' OR t1.industry = '')
AND t2.industry <> 'Null';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


SELECT *
FROM layoffs_staging2;


-- OPTIONAL (to delete the null values)

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off = 'None'
AND percentage_laid_off = 'None';

DELETE 
FROM layoffs_staging2
WHERE total_laid_off = 'None'
AND percentage_laid_off = 'None';


-- REMOVE COLUMNS NOT NEEDED
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


-- DONE




