var csv = require('fast-csv'),
  fs = require('fs'),
  _ = require('underscore'),
  request = require('request');

var FILENAME_IN = 'data.txt',
  FILENAME_OUT = 'data-geocoded.csv',
  API_HOSTNAME = 'http://api.phila.gov/ulrs-secret/v3/',
  rows = [],
  headers = [
    'ticket_number',
    'issue_date',
    'issue_time',
    'reg_state',
    'reg_tag',
    'police_district',
    'location',
    'violation',
    'fine',
    'agency'
  ];

fs.createReadStream(FILENAME_IN)
.pipe(csv({
  delimiter: '\t',
  headers: false,
  ignoreEmpty: true
}))
.on('data', function(row) {
  row = _.object(headers, row); // Add headers to row
  row.sanitized_location = sanitizeAddress(row.location);
  rows.push(row); // Add the row to the array of rows
})
.on('end', function() {
  var pendingRequests = 0,
    errors = 0; // For reporting count back to user at the end

  // Loop through each row
  rows.forEach(function(row) {
    pendingRequests++; // Increment pendingrequest count

    request({
      url: API_HOSTNAME + 'addresses/' + encodeURIComponent(row.sanitized_location),
      qs: { format: 'json', srid: '4326' },
      json: true
    }, function(err, response, body) {
      // If the request library throws an error
      if(err) {
        errors++;
        row.error = 'Request error';
      } // Or if the server returns a non 200 status code
      else if(response.statusCode != 200) {
        errors++;
        row.error = 'Status Code ' + response.statusCode;
      } // Or if there's no addresses array or it's empty
      else if(body.addresses === undefined || ! body.addresses.length) {
        errors++;
        row.error = 'No Addresses Returned';
      } // Otherwise treat it as success
      else {
        var match = body.addresses[0];
        row.error = null;
        row.standardized_address = match.standardizedAddress;
        row.geocode_score = match.similarity;
        row.segment_id = match.segmentId;
        row.lng = match.xCoord;
        row.lat = match.yCoord;
      }
      pendingRequests--; // Decrement pending request count

      // If no more pending requests, log the data to a file
      if( ! pendingRequests) logData();
    });
  });

  // Called when there are no more pending requests
  var logData = function() {
    var outFile = fs.createWriteStream(FILENAME_OUT);
    csv.write(rows, {headers: true}).pipe(outFile);
    console.log('File written to "' + FILENAME_OUT + '" with ' + errors + ' errors.');
  };
});

// Put address cleanup in here
var sanitizeAddress = function(address) {
  // Remove trailing side of block
  address = address.replace(/ (NS|ES|SS|WS)$/, '');

  // Remove BLK
  address = address.replace(' BLK ', ' ');

  // Remove asterisks
  address = address.replace('*', '');

  // Replace UNIT with 1
  address = address.replace(/^UNIT /, '1 ');

  return address;
};
