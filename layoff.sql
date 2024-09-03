
SELECT *
FROM layoffs;

CREATE TABLE layoff_staging
LIKE layoffs;

SELECT*
FROM layoff_staging;

INSERT layoff_staging
SELECT*
FROM layoffs;

SELECT*,
ROW_NUMBER() OVER( 
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs;

WITH duplicate_CTE AS
(
SELECT*,
ROW_NUMBER() OVER( 
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs
)
SELECT*
FROM duplicate_CTE
WHERE row_num > 1;

SELECT*
FROM layoff_staging
WHERE company = 'Casper';

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT*
FROM layoff_staging2;


INSERT INTO layoff_staging2
SELECT*,
ROW_NUMBER() OVER( 
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoff_staging;

SELECT*
FROM layoff_staging2
WHERE row_num >1;

SELECT*
FROM layoff_staging2;

UPDATE layoff_staging2
SET company = trim(company);

SELECT *
FROM layoff_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoff_staging2
ORDER BY 1;



SELECT*
FROM layoff_staging2
WHERE country LIKE 'United States%';

UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

UPDATE layoff_staging2
SET industry = null
WHERE industry = '';

SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT st1.industry, st2.industry
FROM layoff_staging2 st1
JOIN layoff_staging2 st2
	ON st1.company = st2.company
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;

UPDATE layoff_staging2 st1
JOIN layoff_staging2 st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE st1.industry IS NULL
AND st2.industry IS NOT NULL;

SELECT *
FROM layoff_staging2
WHERE industry IS NULL
OR industry = '';

SELECT*
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM layoff_staging2;

ALTER TABLE layoff_staging2
DROP column row_num;


























