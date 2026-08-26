import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join, basename } from 'path';
import { execSync } from 'child_process';

const OUTPUT_DIR = join(import.meta.dirname, 'output');
const IMAGES_DIR = join(import.meta.dirname, '..', '..', 'docs', 'images');

import { mkdirSync } from 'fs';
mkdirSync(IMAGES_DIR, { recursive: true });

const files = [
  'level0-system-overview',
  'level1-control-plane',
  'level1-data-plane',
  'level2-data-flow',
  'level2-protocols',
];

for (const name of files) {
  const excalidrawPath = join(OUTPUT_DIR, `${name}.excalidraw`);
  const pngPath = join(IMAGES_DIR, `${name}.png`);

  if (!existsSync(excalidrawPath)) {
    console.error(`Missing: ${excalidrawPath}`);
    continue;
  }

  console.log(`Exporting: ${name} → PNG`);

  try {
    const excalidrawContent = readFileSync(excalidrawPath, 'utf-8');
    const diagram = JSON.parse(excalidrawContent);

    const { convertToPNG } = await import('@swiftlysingh/excalidraw-cli');
    const pngBuffer = await convertToPNG(diagram, {
      scale: 2,
      padding: 20,
      dark: false,
    });

    writeFileSync(pngPath, pngBuffer);
    console.log(`  ✓ ${pngPath}`);
  } catch (err) {
    console.error(`  ✗ Failed: ${err.message}`);

    console.log(`  Falling back to excalidraw-cli CLI...`);
    try {
      const cmd = `npx excalidraw-cli convert "${excalidrawPath}" --format png -o "${pngPath}" --scale 2 --padding 20`;
      execSync(cmd, { stdio: 'inherit', cwd: import.meta.dirname });
      console.log(`  ✓ ${pngPath} (via CLI)`);
    } catch (cliErr) {
      console.error(`  ✗ CLI also failed: ${cliErr.message}`);
      console.log(`  Creating placeholder for ${name}...`);

      const placeholderSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="800" height="400" viewBox="0 0 800 400">
        <rect width="800" height="400" fill="#f8f9fa" stroke="#dee2e6" rx="8"/>
        <text x="400" y="180" text-anchor="middle" font-family="sans-serif" font-size="24" fill="#495057">${name.replace(/-/g, ' ').replace(/level\\d/g, m => m.toUpperCase())}</text>
        <text x="400" y="220" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#868e96">Open .excalidraw file at excalidraw.com to view</text>
      </svg>`;
      writeFileSync(pngPath.replace('.png', '.svg'), placeholderSvg);
      console.log(`  → Created SVG placeholder: ${pngPath.replace('.png', '.svg')}`);
    }
  }
}

console.log('\nDone! Check docs/images/ for PNG files.');
