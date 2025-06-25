const fs = require('fs');
const { execSync } = require('child_process');
const randomUseragent = require('random-useragent');
const Bottleneck = require('bottleneck');
const limiter = new Bottleneck({
	maxConcurrent: 10,
});

const belgiumExtent = {
	xmin: 2.54,   // West (longitude)
	ymin: 49.49,  // South (latitude)
	xmax: 6.42,   // East (longitude)
	ymax: 51.51   // North (latitude)
};

// const belgiumExtent = {
// 	ymin: 49.95,
// 	xmin: 5.15,
// 	ymax: 50,
// 	xmax: 5.2,
// }

const params = {
	service: 'WFS',
	version: '2.0.0',
	request: 'GetFeature',
	typename: 'CL:Cadastral_parcel',
	outputFormat: 'GML3',
	srsName: 'EPSG:4326',
	bbox: '50.84,4.33,50.86,4.36,EPSG:4326'  // Ymin,Xmin,Ymax,Xmax
};

function generateTiles({ xmin, ymin, xmax, ymax }, step = 0.05) {
	const tiles = [];
	for (let y = ymin; y < ymax; y += step) {
		for (let x = xmin; x < xmax; x += step) {
			tiles.push({
				bbox: [
					roundCoord(y),
					roundCoord(x),
					roundCoord(y + step),
					roundCoord(x + step)
				]
			});
		}
	}
	return tiles;
}

function roundCoord(value, decimals = 6) {
	return Number(value.toFixed(decimals));
}

async function fetchAndConvertTile(bbox, tileId) {
	const BASE_URL = 'http://ccff02.minfin.fgov.be/geoservices/arcgis/services/WMS/Cadastral_LayersWFS/MapServer/WFSServer';

	const params = {
		service: 'WFS',
		version: '2.0.0',
		request: 'GetFeature',
		typename: 'CL:Cadastral_parcel',
		outputFormat: 'GML3',
		srsName: 'EPSG:4326',
		bbox: bbox.join(',') + ',EPSG:4326',
		count: 10_000
	};

	const url = `${BASE_URL}?${new URLSearchParams(params)}`;

	const gmlPath = `tiles/tile_${tileId}.gml`;
	const gpkgPath = `tiles/tile_${tileId}.gpkg`;

	return gpkgPath;

	if (fs.existsSync(gpkgPath)) {
		console.log(`🟢 ${tileId} already exists`);
		return gpkgPath
	}

	try {
		console.log(`🟡 Fetching tile ${tileId}`, url);
		const res = await fetch(url, {
			headers: {
				'User-Agent': `Please-Provide-SuVaCn-As-A-Dataset-So-I-Dont-Have-To-Scrape (felikx.be)`
			}
		});
		if (!res.ok) throw new Error(`HTTP ${res.status}`);

		const xml = await res.text();
		fs.writeFileSync(gmlPath, xml);

		console.log(`🟢 Converting tile ${tileId} to GPKG...`);
		execSync(`ogr2ogr -f GPKG -s_srs EPSG:4326 -t_srs EPSG:4326 -ct "+proj=pipeline +step +proj=axisswap +order=2,1" ${gpkgPath} ${gmlPath} -nln parcels`, { stdio: 'inherit' });

		fs.unlinkSync(gmlPath); // optional: clean up
		return gpkgPath;
	} catch (err) {
		console.error(`❌ Tile ${tileId} failed: ${err.message} for ${url}`);
		return null;
	}
}

(async () => {
	const tiles = generateTiles(belgiumExtent, 0.01);
	console.log('Tiles length', tiles.length);
	const results = await Promise.all(
		tiles.map((tile, i) =>
			limiter.schedule(() => fetchAndConvertTile(tile.bbox, tile.bbox.join('_')))
		)
	);


	// Merge them all
	const finalGpkg = 'belgium_merged.gpkg';

	if (fs.existsSync(finalGpkg)) {
		fs.unlinkSync(finalGpkg);
	}

	execSync(`echo '<OGRVRTDataSource></OGRVRTDataSource>' > tiles/empty.vrt`)
	execSync(`ogr2ogr -f GPKG ${finalGpkg} tiles/empty.vrt`)

	console.log('🔄 Merging all GPKGs...');
	for (let i = 0; i < tiles.length; i++) {
		const tileFile = `tiles/tile_${tiles[i].bbox.join('_')}.gpkg`;

		// Check if the parcels layer exists in this file
		try {
			execSync(`ogrinfo "${tileFile}" parcels`, { stdio: 'ignore' });
			console.log(`[${i}/${tiles.length}] ✅ Layer 'parcels' found in ${tileFile}`);

			const cmd = `ogr2ogr -f GPKG -update -append "${finalGpkg}" "${tileFile}" -nln parcels parcels`;
			execSync(cmd, { stdio: 'inherit' });

		} catch (error) {
			console.log(`[${i}/${tiles.length}] ⚠️  Layer 'parcels' not found in ${tileFile}, skipping...`);
		}
	}

	console.log('✅ All tiles processed and merged!');
})();