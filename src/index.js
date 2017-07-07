const _ = require( 'lodash' );
const AWS = require( 'aws-sdk' );
const moment = require( 'moment' );
const Promise = require( 'bluebird' );
const request = require( 'request-promise' );
const urlLib = require( 'url' );

function createElasticSearchClient( 
		elasticsearchUrl,
		awsRegion,
		awsCredentials
	) {

	const esClient = request.defaults( {
		baseUrl: elasticsearchUrl
	} );

	const esUrlInfo = urlLib.parse( elasticsearchUrl );

	return ( req ) => {

		const signingReq = new AWS.HttpRequest( elasticsearchUrl );
		signingReq.method = req.method;
		signingReq.path = req.url;
		signingReq.region = awsRegion;

		if( !signingReq.headers ) {
			signingReq.headers = {};
		}
		signingReq.headers.Host = esUrlInfo.hostname;

		const signer = new AWS.Signers.V4( signingReq, 'es' );
		signer.addAuthorization( awsCredentials, new Date() );

		if( !req.headers ) {
			req.headers = {};
		}

		_.forIn( signingReq.headers, ( value, key ) => {
			req.headers[ key ] = value;
		} );

		return esClient( req );
	};
}

function getAwsCredentialsProviderAsync() {

	return new Promise( ( resolve, reject ) => {

		const chain = new AWS.CredentialProviderChain();
		chain.resolve( ( resolveErr, provider ) => {

			if( resolveErr ) {
				return reject( resolveErr );
			}

			return provider.get( getErr => {

				if( getErr ) {
					return reject( getErr );
				}
				
				return resolve( provider );
			} );
		} );
	} );
}

function deleteExpiredIndiciesAsync( args ) {

	return getAwsCredentialsProviderAsync()
		.then( awsCredentials => {
			
			const esClient = createElasticSearchClient(
					args.elasticsearchUrl,
					args.awsRegion,
					awsCredentials
				);

			return esClient( { 
					method: 'GET', 
					url: '/_cluster/state/metadata',
					json: true
				} )
				.then( clusterInfo => {

					const targetIndicies = _.pickBy( clusterInfo.metadata.indices, ( metadata, name ) => {
						return args.indexFilter( name );
					} );

					const targetIndexNames = _.keys( targetIndicies );

					return Promise.map(
							targetIndexNames,
							indexName => {
								
								console.log( `Deleting index '${ indexName }'` );
								
								return esClient( { 
										method: 'DELETE', 
										url: `/${ indexName }`,
										json: true
									} )
									.then( result => {
										if( result.acknowledged ) {
											console.log( `Elasticsearch has acknowledged the request to delete index '${ indexName }'` );
										} else {
											console.warn( `Elasticsearch has not acknowledged the request to delete index '${ indexName }'` );
										}
									} )
									.catch( err => {
										console.error( `Failed to delete index '${ indexName }':`, err.message );
										throw( err );
									} );
							}, 
							{ concurrency: 1 } 
						)
						.then( () => {
							return targetIndexNames;
						} );
				} );
		} );
}

// ------------------------------------------------------------

const awsRegion = process.env.AWS_REGION;
if( !awsRegion ) {
	console.error( 'AWS_REGION not set' );
	process.exit( 1 );
}

const elasticsearchUrl = process.env.ELASTICSEARCH_URL;
if( !elasticsearchUrl ) {
	console.error( 'ELASTICSEARCH_URL not set' );
	process.exit( 2 );
}

return deleteExpiredIndiciesAsync( {
		awsRegion,
		elasticsearchUrl,
		indexFilter: ( indexName ) => {
			return ( indexName !== '.kibana' );
		}
	})
	.then( result => {
		process.exit( 0 );
	})
	.catch( err => {
		console.error( err );
		process.exit( 100 );
	} );