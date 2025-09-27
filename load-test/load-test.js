const autocannon = require('autocannon');

// Generate timestamps for specific hours
function generateTimestampForHour(hourOffset) {
  const now = new Date();
  // Set the time to the beginning of the specified hour
  const targetTime = new Date(now);
  targetTime.setHours(now.getHours() - hourOffset);
  targetTime.setMinutes(Math.floor(Math.random() * 60)); // Random minute within the hour
  targetTime.setSeconds(Math.floor(Math.random() * 60)); // Random second within the minute
  return targetTime.toISOString();
}

// Generate random page URLs
const pages = [
  '/home',
  '/about',
  '/products',
  '/blog',
  '/contact',
  '/pricing',
  '/features',
  '/docs',
  '/api',
  '/login'
];

// Generate test data with views for all 24 hours
function generateRequestBody() {
  // Generate batch of page views across all pages and 24 hours
  const batch = {};
  
  // Select 3-5 random pages
  const numPages = Math.floor(Math.random() * 3) + 3; // 3-5 pages
  const selectedPages = [];
  
  // Select random pages without duplicates
  while (selectedPages.length < numPages) {
    const randomPage = pages[Math.floor(Math.random() * pages.length)];
    if (!selectedPages.includes(randomPage)) {
      selectedPages.push(randomPage);
    }
  }
  
  // For each selected page, add data for all 24 hours
  for (const page of selectedPages) {
    if (!batch[page]) {
      batch[page] = {};
    }
    
    // Add data for all 24 hours
    for (let hour = 0; hour < 24; hour++) {
      const timestamp = generateTimestampForHour(hour);
      // Number of views for this page at this hour (between 50 and 500)
      batch[page][timestamp] = Math.floor(Math.random() * 451) + 50;
    }
  }
  
  return JSON.stringify(batch);
}

// Let's run multiple iterations to make sure we hit all pages with all hours
function runLoadTest() {
  // For debug purposes, let's log a sample request to see what we're sending
  const sampleRequest = generateRequestBody();
  console.log('Sample request payload:');
  console.log(sampleRequest);

  const instance = autocannon({
    url: 'http://localhost:3000/page-views/multi',
    connections: 10,
    pipelining: 1,
    duration: 60,
    headers: {
      'content-type': 'application/json'
    },
    requests: [
      {
        method: 'POST',
        body: generateRequestBody() // Generate a batch for testing
      }
    ]
  });

  // Track progress
  instance.on('response', handleResponse);

  function handleResponse(client) {
    console.log(`${client.statusCode} - ${client.body?.toString() || 'No body'}`);
  }

  // Log results
  autocannon.track(instance, { renderProgressBar: true });

  instance.on('done', (results) => {
    console.log('Load test completed!');
    console.log('Summary:');
    console.log(`Requests/sec: ${results.requests.average}`);
    console.log(`Latency (avg): ${results.latency.average}ms`);
    console.log(`Total requests: ${results.requests.total}`);
    console.log(`2xx responses: ${results.requests.sent - results.non2xx}`);
    console.log(`Non-2xx responses: ${results.non2xx}`);
  });
}

// Run the load test
runLoadTest();
