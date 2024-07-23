-- Exploratory Data Analysis 

SELECT *
FROM layoffs_staging2;

SELECT total_laid_off
FROM layoffs_staging2
WHERE total_laid_off = 'None';

UPDATE layoffs_staging2
SET total_laid_off = NULL 
WHERE total_laid_off = 'None'; 

SELECT total_laid_off
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

ALTER TABLE layoffs_staging2
MODIFY total_laid_off INTEGER;

SELECT max(total_laid_off)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET percentage_laid_off = NULL 
WHERE percentage_laid_off = 'None'; 

SELECT percentage_laid_off
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL; 

SELECT max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY year(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT country, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


SELECT *
FROM layoffs_staging2;


SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
ORDER BY 3 DESC;

WITH company_year (company, years, total_laid_off) AS 
(
SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;






















