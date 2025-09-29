const autocannon = require('autocannon');

function generateTimestampForHour(hourOffset) {
  const now = new Date();
  const targetTime = new Date(now);

  targetTime.setHours(now.getHours() - hourOffset);

  targetTime.setMinutes(Math.floor(Math.random() * 60));
  targetTime.setSeconds(Math.floor(Math.random() * 60));

  return targetTime.toISOString();
}

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
  '/login',
];

// Build a request body with 3–5 random pages × 24 hours
function generateRequestBody() {
  const batch = {};
  const numPages = Math.floor(Math.random() * 3) + 3;
  const selectedPages = [];

  while (selectedPages.length < numPages) {
    const randomPage = pages[Math.floor(Math.random() * pages.length)];
    if (!selectedPages.includes(randomPage)) {
      selectedPages.push(randomPage);
    }
  }

  for (const page of selectedPages) {
    batch[page] = {};

    for (let hour = 0; hour < 24; hour++) {
      const timestamp = generateTimestampForHour(hour);
      batch[page][timestamp] = Math.floor(Math.random() * 451) + 50; // 50–500 views
    }
  }

  return JSON.stringify(batch);
}

function runLoadTest() {
  console.log('Sample request payload:\n', generateRequestBody());

  const instance = autocannon({
    url: 'http://localhost:3000/page-views/multi',
    connections: 10,     // concurrent users
    pipelining: 1,       // requests per connection
    duration: 60,        // seconds
    headers: {
      'content-type': 'application/json',
    },
    requests: [
      {
        method: 'POST',
        setupRequest: (req) => {
          req.body = generateRequestBody();
          return req;
        },
      },
    ],
  });

  instance.on('response', (client) => {
    console.log(`${client.statusCode} - ${client.body?.toString() || 'No body'}`);
  });

  autocannon.track(instance, { renderProgressBar: true });

  instance.on('done', (results) => {
    console.log('\nLoad test completed!');
    console.log('Summary:');
    console.log(`Requests/sec: ${results.requests.average}`);
    console.log(`Latency (avg): ${results.latency.average}ms`);
    console.log(`Total requests: ${results.requests.total}`);
    console.log(`2xx responses: ${results.requests.sent - results.non2xx}`);
    console.log(`Non-2xx responses: ${results.non2xx}`);
  });
}

runLoadTest();
