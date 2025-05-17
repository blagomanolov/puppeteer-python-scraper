import puppeteer from 'puppeteer';
import fs from 'fs/promises';

(async () => {
    const browser = await puppeteer.launch({
        headless: 'new', // Or true for older Puppeteer versions
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    })

    const page = await browser.newPage();

    await page.goto('https://www.worldometers.info/world-population/population-by-region/', { waitUntil: 'networkidle2' });

    await page.waitForSelector("button[class*='fc-button fc-cta-consent fc-primary-button']");
    await page.click("button[class*='fc-button fc-cta-consent fc-primary-button']");

    await page.waitForSelector("table[class*='datatable w-full border border-zinc-200 datatable-table']");

    let tableHtml = await page.evaluate(() =>
        document.querySelector("table[class*='datatable w-full border border-zinc-200 datatable-table']").outerHTML
    );
    await fs.writeFile('/app/parser/table.html', tableHtml); // Absolute path
    console.log('Table saved to table.html');
    await browser.close();
})();
