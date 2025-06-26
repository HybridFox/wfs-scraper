const fs = require('fs');
const { execSync , spawn} = require('child_process');
const path = require('path');
const randomUseragent = require('random-useragent');
const Bottleneck = require('bottleneck');
const limiter = new Bottleneck({
	maxConcurrent: 5,
});

// const belgiumExtent = {
// 	xmin: 5.7,   // West (longitude)
// 	ymin: 49.9,  // South (latitude)
// 	xmax: 5.9,   // East (longitude)
// 	ymax: 50   // North (latitude)
// };

const belgiumExtent = {
	ymin: 49.49,
	xmin: 2.54,
	ymax: 51.51,
	xmax: 6.42,
}

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

	// await Promise.all(
	// 	tiles.map((tile, i) =>
	// 		limiter.schedule(() => fetchAndConvertTile(i,  tiles.length, tile.bbox, tile.bbox.join('_')))
	// 	)
	// );


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
        .map(file => path.join(tilesDir, file));

    console.log(`Found ${gpkgFiles.length} GPKG files`);

    // Optimized validation using parallel processing
    console.log('🔄 Validating GPKGs in parallel...');
    const validGpkgs = await validateGpkgsInParallel(gpkgFiles, 500); // Process 50 at a time
    
    console.log(`Found ${validGpkgs.length} valid GPKGs out of ${gpkgFiles.length} total files`);

    if (validGpkgs.length === 0) {
        console.log('❌ No valid GPKG files found with parcels layer');
        return;
    }

    // Process in batches to avoid command line length limits and memory issues
    const batchSize = 1000; // Adjust based on your system    
    console.log(`🔄 Processing ${validGpkgs.length} files in batches of ${batchSize}...`);
    
    for (let i = 0; i < validGpkgs.length; i += batchSize) {
        const batch = validGpkgs.slice(i, i + batchSize);
        const batchNum = Math.floor(i / batchSize) + 1;
        const totalBatches = Math.ceil(validGpkgs.length / batchSize);
        
        console.log(`🔄 Processing batch ${batchNum}/${totalBatches} (${batch.length} files)...`);
        
        const vrtPath = `out/batch_${batchNum}.vrt`;
        
        try {
            // Create VRT for this batch
            createVrtFile(batch, vrtPath);
            
            // Merge this batch
            const appendFlag = i > 0 ? '-append' : ''; // Don't append for first batch
            const cmd = `ogr2ogr -f GPKG "${finalGpkg}" "${vrtPath}" -nln parcels ${appendFlag} --config OGR_SQLITE_SYNCHRONOUS OFF --config OGR_SQLITE_CACHE 2048 --config OGR_SQLITE_TEMP_STORE MEMORY`;
            
            execSync(cmd, { stdio: 'inherit' });
            
            // Clean up batch VRT
            fs.unlinkSync(vrtPath);
            
        } catch (error) {
            console.error(`❌ Error processing batch ${batchNum}:`, error.message);
            // Clean up on error
            if (fs.existsSync(vrtPath)) {
                fs.unlinkSync(vrtPath);
            }
            throw error;
        }
    }
    
    console.log('✅ All tiles processed and merged successfully!');
	console.log('🔄 deduping GPKG...');
	execSync(`ogr2ogr -f GPKG "${dedupedGpkg}" "${finalGpkg}" -nln parcels -sql "SELECT * FROM parcels WHERE ROWID IN (SELECT MIN(ROWID) FROM parcels GROUP BY CaPaKey)" -dialect sqlite`, { stdio: 'inherit' });

	console.log('🟢 Done!', dedupedGpkg);
})();

// Optimized parallel validation function
async function validateGpkgsInParallel(gpkgFiles, concurrency = 50) {
    const validGpkgs = [];
    const chunks = [];
    
    // Split files into chunks for parallel processing
    for (let i = 0; i < gpkgFiles.length; i += concurrency) {
        chunks.push(gpkgFiles.slice(i, i + concurrency));
    }
    
    let processedCount = 0;
    
    for (const chunk of chunks) {
        const promises = chunk.map(async (gpkgPath) => {
            try {
                // Use async spawn instead of sync execSync for better performance
                return await checkGpkgHasParcels(gpkgPath);
            } catch (error) {
                return null;
            }
        });
        
        const results = await Promise.all(promises);
        
        // Collect valid results
        results.forEach((result, index) => {
            if (result) {
                validGpkgs.push(chunk[index]);
            }
        });
        
        processedCount += chunk.length;
        console.log(`Validated: ${processedCount}/${gpkgFiles.length} files`);
    }
    
    return validGpkgs;
}

// Async function to check if GPKG has parcels layer
function checkGpkgHasParcels(gpkgPath) {
    return new Promise((resolve, reject) => {
        const child = spawn('ogrinfo', [gpkgPath, 'parcels'], {
            stdio: ['ignore', 'ignore', 'ignore']
        });
        
        child.on('close', (code) => {
            if (code === 0) {
                resolve(gpkgPath);
            } else {
                reject(new Error(`No parcels layer in ${gpkgPath}`));
            }
        });
        
        child.on('error', (error) => {
            reject(error);
        });
    });
}

// Create VRT file for a batch of GPKGs
function createVrtFile(gpkgPaths, vrtPath) {
    const vrtContent = ['<OGRVRTDataSource>'];
    
    gpkgPaths.forEach(gpkgPath => {
        // Use absolute paths to avoid issues
        const absolutePath = path.resolve(gpkgPath);
        vrtContent.push(`
    <OGRVRTLayer name="parcels">
        <SrcDataSource>${absolutePath}</SrcDataSource>
        <SrcLayer>parcels</SrcLayer>
    </OGRVRTLayer>`);
    });
    
    vrtContent.push('</OGRVRTDataSource>');
    fs.writeFileSync(vrtPath, vrtContent.join('\n'));
}
