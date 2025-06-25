const fs = require('fs');
const { execSync } = require('child_process');
const randomUseragent = require('random-useragent');
const Bottleneck = require('bottleneck');
const limiter = new Bottleneck({
	maxConcurrent: 5,
});

const belgiumExtent = {
	xmin: 5.7,   // West (longitude)
	ymin: 49.9,  // South (latitude)
	xmax: 5.9,   // East (longitude)
	ymax: 50   // North (latitude)
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

async function fetchWithRetry(url, options = {}, maxRetries = 3, baseDelay = 1000) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const response = await fetch(url, options);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            return response;
        } catch (error) {
            if (attempt === maxRetries - 1) {
                throw error; // Last attempt failed, propagate the error
            }
            const delay = baseDelay * Math.pow(2, attempt);
            console.log(`Retry attempt ${attempt + 1}/${maxRetries} after ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

async function fetchAndConvertTile(rootIndex, totalLength, bbox, tileId, depth = 0) {
    const BASE_URL = 'http://ccff02.minfin.fgov.be/geoservices/arcgis/services/WMS/Cadastral_LayersWFS/MapServer/WFSServer';
    const maxDepth = 5;

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
        const res = await fetchWithRetry(url, {
            headers: {
                'User-Agent': `Please-Provide-SuVaCn-As-A-Dataset-So-I-Dont-Have-To-Scrape (felikx.be)`
            }
        });

        const xml = await res.text();
        
        const featureCount = (xml.match(/<CL:Cadastral_parcel/g) || []).length;
        console.log(`[${rootIndex}/${totalLength}] 🟡 Got featureCountLength for tile ${tileId}: ${featureCount}`);
        
        if (featureCount >= 400 && depth < maxDepth) {
            console.log(`[${rootIndex}/${totalLength}] 📦 Tile ${tileId} has ${featureCount} features (limit reached). Splitting...`);
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

        fs.unlinkSync(gmlPath);
        return [gpkgPath];
    } catch (err) {
        console.error(`❌ Tile ${tileId} failed: ${err.message} for ${url}`);
        return [];
    }
}

(async () => {
	const tiles = generateTiles(belgiumExtent, 0.01);
	console.log('Tiles length', tiles.length);


	if (!fs.existsSync('out')) {
		fs.mkdirSync('out');
	}

	if (!fs.existsSync('tiles')) {
		fs.mkdirSync('tiles');
	}

	await Promise.all(
		tiles.map((tile, i) =>
			limiter.schedule(() => fetchAndConvertTile(i,  tiles.length, tile.bbox, tile.bbox.join('_')))
		)
	);


	console.log('⌛ Done fetching!');
	// Merge them all
	const finalGpkg = 'out/belgium_merged.gpkg';
	const dedupedGpkg = 'out/belgium_deduped.gpkg';

	if (fs.existsSync(finalGpkg)) {
		fs.unlinkSync(finalGpkg);
	}

	console.log('🔄 Reading tiles directory...');
	const tilesDir = 'tiles';
	const gpkgFiles = fs.readdirSync(tilesDir)
		.filter(file => file.endsWith('.gpkg'))
		.map(file => `${tilesDir}/${file}`);

	// Create a VRT file that references all valid GPKGs
	console.log(`Found ${gpkgFiles.length} GPKG files`);

	// Create a VRT file that references all valid GPKGs
	console.log('🔄 Creating VRT file...');
	const vrtContent = [`<OGRVRTDataSource>`];
	
	// Filter and validate GPKGs
	const validGpkgs = gpkgFiles;
	// let processedCount = 0;
	// for (const gpkgPath of gpkgFiles) {
	// 	processedCount++;
	// 	if (processedCount % 100 === 0) {
	// 		console.log(`Validating GPKGs: ${processedCount}/${gpkgFiles.length}`);
	// 	}

	// 	try {
	// 		execSync(`ogrinfo "${gpkgPath}" parcels`, { stdio: 'ignore' });
	// 		validGpkgs.push(gpkgPath);
	// 	} catch (error) {
	// 		console.log(`⚠️  Layer 'parcels' not found in ${gpkgPath}, skipping...`);
	// 	}
	// }

	console.log(`Found ${validGpkgs.length} valid GPKGs out of ${gpkgFiles.length} total files`);

	// Add all valid GPKGs to the VRT
	validGpkgs.forEach(gpkgPath => {
		vrtContent.push(`
    <OGRVRTLayer name="parcels">
        <SrcDataSource>${gpkgPath}</SrcDataSource>
        <SrcLayer>parcels</SrcLayer>
    </OGRVRTLayer>`);
	});
	
	vrtContent.push('</OGRVRTDataSource>');
	
	// Write the VRT file
	const vrtPath = 'tiles/merge.vrt';
	fs.writeFileSync(vrtPath, vrtContent.join('\n'));

	// Perform single merge operation
	console.log('🔄 Merging all GPKGs in one operation...');
	try {
		execSync(`ogr2ogr -f GPKG "${finalGpkg}" "${vrtPath}" -nln parcels -append --config OGR_SQLITE_SYNCHRONOUS OFF --config OGR_SQLITE_CACHE 1024`, { stdio: 'inherit' });
		console.log('✅ All tiles processed and merged!');
	} catch (error) {
		console.error('❌ Error during merge:', error);
	} finally {
		// Clean up
		fs.unlinkSync(vrtPath);
	}

	console.log('🔄 deduping GPKG...');
	execSync(`ogr2ogr -f GPKG "${dedupedGpkg}" "${finalGpkg}" -nln parcels -sql "SELECT * FROM parcels WHERE ROWID IN (SELECT MIN(ROWID) FROM parcels GROUP BY CaPaKey)" -dialect sqlite`, { stdio: 'inherit' });

	console.log('🟢 Done!', dedupedGpkg);
})();

// ogr2ogr -f GPKG belgium_deduped.gpkg belgium_merged.gpkg   -nln parcels   -sql "SELECT * FROM parcels WHERE ROWID IN (SELECT MIN(ROWID) FROM parcels GROUP BY CaPaKey)"   -dialect sqlite