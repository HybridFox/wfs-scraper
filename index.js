const fs = require('fs');
const { execSync } = require('child_process');
const randomUseragent = require('random-useragent');
const Bottleneck = require('bottleneck');
const limiter = new Bottleneck({
	maxConcurrent: 5,
});

const belgiumExtent = {
	xmin: 4.2,   // West (longitude)
	ymin: 50.8,  // South (latitude)
	xmax: 4.4,   // East (longitude)
	ymax: 50.9   // North (latitude)
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

function splitBbox(bbox) {
    const [ymin, xmin, ymax, xmax] = bbox;
    const ymid = (ymin + ymax) / 2;
    const xmid = (xmin + xmax) / 2;
    
    return [
        [ymin, xmin, ymid, xmid],    // Southwest
        [ymin, xmid, ymid, xmax],    // Southeast
        [ymid, xmin, ymax, xmid],    // Northwest
        [ymid, xmid, ymax, xmax]     // Northeast
    ].map(coords => coords.map(c => roundCoord(c)));
}

async function fetchAndConvertTile(rootIndex, totalLength, bbox, tileId, depth = 0) {
	const BASE_URL = 'http://ccff02.minfin.fgov.be/geoservices/arcgis/services/WMS/Cadastral_LayersWFS/MapServer/WFSServer';
	const maxDepth = 5; // Prevent infinite recursion

	const params = {
		service: 'WFS',
		version: '2.0.0',
		request: 'GetFeature',
		typename: 'CL:Cadastral_parcel',
		outputFormat: 'GML3',
		srsName: 'EPSG:4326',
		bbox: bbox.join(',') + ',EPSG:4326',
		count: 1000
	};

	const url = `${BASE_URL}?${new URLSearchParams(params)}`;
	const gmlPath = `tiles/tile_${tileId}.gml`;
	const gpkgPath = `tiles/tile_${tileId}.gpkg`;

	if (fs.existsSync(gpkgPath)) {
		console.log(`[${rootIndex}/${totalLength}] 🟢 ${tileId} already exists`);
		return [gpkgPath];
	}

	try {
		console.log(`[${rootIndex}/${totalLength}] 🟡 Fetching tile ${tileId}`, url);
		const res = await fetch(url, {
			headers: {
				'User-Agent': `Please-Provide-SuVaCn-As-A-Dataset-So-I-Dont-Have-To-Scrape (felikx.be)`
			}
		});

		if (!res.ok) {
			throw new Error(`HTTP ${res.status}`);
		}

		const xml = await res.text();
		
		// Check if the response contains exactly 500 features (the limit)
		const featureCount = (xml.match(/<CL:Cadastral_parcel/g) || []).length;
		console.log(`[${rootIndex}/${totalLength}] 🟡 Got featureCountLength for tile ${tileId}: ${featureCount}`);
		
		if (featureCount >= 400 && depth < maxDepth) {
			console.log(`[${rootIndex}/${totalLength}] 📦 Tile ${tileId} has ${featureCount} features (limit reached). Splitting...`);
			// Split the bbox and recursively fetch - without using bottleneck for sub-requests
			const subBboxes = splitBbox(bbox);
			const subResults = await Promise.all(
				subBboxes.map((subBbox, i) => 
					fetchAndConvertTile(rootIndex, totalLength, subBbox, `${tileId}_${i}`, depth + 1)
				)
			);
	
			return subResults.flat();
		}

		fs.writeFileSync(gmlPath, xml);

		console.log(`[${rootIndex}/${totalLength}] 🟢 Converting tile ${tileId} to GPKG...`);
		execSync(`ogr2ogr -f GPKG -s_srs EPSG:4326 -t_srs EPSG:4326 -ct "+proj=pipeline +step +proj=axisswap +order=2,1" ${gpkgPath} ${gmlPath} -nln parcels`, { stdio: 'inherit' });

		fs.unlinkSync(gmlPath); // optional: clean up
		return [gpkgPath];
	} catch (err) {
		console.error(`❌ Tile ${tileId} failed: ${err.message} for ${url}`);
		return [];
	}
}

(async () => {
	const tiles = generateTiles(belgiumExtent, 0.01);
	console.log('Tiles length', tiles.length);

	const results = (await Promise.all(
		tiles.map((tile, i) =>
			limiter.schedule(() => fetchAndConvertTile(i,  tiles.length, tile.bbox, tile.bbox.join('_')))
		)
	)).flat();


	console.log('⌛ Done fetching!');
	// Merge them all
	const finalGpkg = 'belgium_merged.gpkg';

	if (fs.existsSync(finalGpkg)) {
		fs.unlinkSync(finalGpkg);
	}

	execSync(`echo '<OGRVRTDataSource></OGRVRTDataSource>' > tiles/empty.vrt`)
	execSync(`ogr2ogr -f GPKG ${finalGpkg} tiles/empty.vrt`)

	console.log('🔄 Merging all GPKGs...');
	let currentIndex = 0;
	for (const gpkgPath of results) {
		currentIndex ++;

		if (!gpkgPath) continue;

		// Check if the parcels layer exists in this file
		try {
			execSync(`ogrinfo "${gpkgPath}" parcels`, { stdio: 'ignore' });
			console.log(`[${currentIndex}/${results.length}] ✅ Layer 'parcels' found in ${gpkgPath}`);

			const cmd = `ogr2ogr -f GPKG -update -append "${finalGpkg}" "${gpkgPath}" -nln parcels parcels`;
			execSync(cmd, { stdio: 'inherit' });

		} catch (error) {
			console.log(`[${currentIndex}/${results.length}] ⚠️  Layer 'parcels' not found in ${gpkgPath}, skipping...`);
		}
	}

	console.log('✅ All tiles processed and merged!');
})();