-- Data Cleaning

SELECT * FROM layoffs;

CREATE TABLE layoffs.layoffs_staging
LIKE layoffs.layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs.layoffs;

-- check duplicates
SELECT * FROM layoffs.layoffs_staging
;

SELECT  * FROM (
SELECT company, industry, total_laid_off, 'date',
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off,'date') AS row_num
FROM layoffs.layoffs_staging
) duplicates
WHERE
row_num > 1;

SELECT *
FROM layoffs.layoffs_staging
WHERE company = 'Oda';

SELECT * 
FROM (
SELECT company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
) AS row_num
FROM layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

ALTER TABLE layoffs.layoffs_staging
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;
SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs.layoffs_staging
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
                   ORDER BY id
               ) AS row_num
        FROM layoffs.layoffs_staging
    ) ranked
    WHERE row_num > 1
);

SELECT *
FROM layoffs.layoffs_staging 
;

CREATE TABLE `layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO layoffs.layoffs_staging2 (
  company,
  location,
  industry,
  total_laid_off,
  percentage_laid_off,
  date,
  stage,
  country,
  funds_raised_millions,
  row_num
)
SELECT 
  company,
  location,
  industry,
  total_laid_off,
  percentage_laid_off,
  date,
  stage,
  country,
  funds_raised_millions,
  ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
  ) AS row_num
FROM layoffs.layoffs_staging;

DELETE FROM layoffs.layoffs_staging2
WHERE row_num >= 2;

-- standardise data

SELECT * 
FROM layoffs.layoffs_staging2;
-- we have null values, we have to fix that

SELECT DISTINCT industry
FROM layoffs.layoffs_staging2
ORDER BY industry;

SELECT * 
FROM layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = ' '
ORDER BY industry;

SELECT *
FROM layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

UPDATE layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = ' ';

SELECT * 
FROM layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = ' '
ORDER BY industry;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = ' '
ORDER BY industry;

SELECT DISTINCT industry
FROM layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry in ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT industry
FROM layoffs.layoffs_staging2
ORDER BY industry;

-- as well we need to look at

SELECT * 
FROM layoffs.layoffs_staging2;
-- united states is done differently. Let's fix it
SELECT DISTINCT country
FROM layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- let's see if it is fixed
SELECT DISTINCT country
FROM layoffs.layoffs_staging2
ORDER BY country;

-- Let's fix the date columns
SELECT * 
FROM layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs.layoffs_staging2;

-- we're not changing the null values for now
-- now we're removing any columns and rows we need to

SELECT * 
FROM layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- now we're going to delete useless data we can't use
DELETE FROM layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs.layoffs_staging2;