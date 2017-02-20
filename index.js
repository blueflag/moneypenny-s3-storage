var AWS = require('aws-sdk');
var s3 = new AWS.S3();

function get(bucket, id){
    return new Promise((resolve, reject) => {
        s3.getObject({
            Bucket: bucket,
            Key: id
        }
        ,(err, data) => {
            if(err){
                return reject(err);
            }
            else resolve(JSON.parse(data));
        });
    })
}

function put(bucket, id, data){
    return new Promise((resolve, reject) => {
        s3.putObject({
            Bucket: bucket,
            Key: id,
            Body: JSON.stringify(client)
        }
        ,(err, data) => {
            if(err){
                return reject(err);
            }
            else resolve(data);
        });
    })
}

function remove(bucket, id){
    return new Promise((resolve, reject) => {
        s3.deleteObject({
            Bucket: bucket,
            Key: id
        }
        ,(err, data) => {
            if(err){
                return reject(err);
            }
            else resolve();
        });
    })
}

/**
 * @param prefix {String} prefix at the start of the collection.
 *
 */
function tokenStore(prefix, bucket){
    return{
        save: function(refreshToken){
            return Promise.resolve(refreshToken)
                .then((refreshToken) => {
                    return put(bucket, `${prefix}/client-user/${refreshToken.userId}-${refreshToken.clientId}`, refreshToken);
                })
                .then((refreshToken) => {
                    return put(bucket, `${prefix}/token/${refreshToken.token}`, refreshToken);
                });
        },
        fetchByToken: function(token){
            return get(bucket, `${prefix}/token/${token}`);
        },
        removeByUserIdClientId: function(userId, clientId){
            return get(bucket, `${prefix}/token/${userId}-${clientId}`)
                .then((refreshToken)=> {
                    return remove(bucket, `${prefix}/client-user/${refreshToken.userId}-${refreshToken.clientId}`)
                    .then(()=> {
                        return remove(bucket, `${prefix}/token/${refreshToken.token}`)
                    })
                });
        },
        removeByRefreshToken: function(token){
            return get(bucket, `${prefix}/token/${token}`)
                .then((refreshToken)=> {
                    return remove(bucket, `${prefix}/client-user/${refreshToken.userId}-${refreshToken.clientId}`)
                    .then(()=> {
                        return remove(bucket, `${prefix}/token/${refreshToken.token}`)
                    })
                });
        }
    }
}

module.exports = function(options){
    const bucket = options.bucket;
    return {
        clientStore: {
            createClient: (client) => {
                return put(bucket, `clients/${client.id}`, client)
            },
            fetchById: (clientId) => {
                return get(bucket, `clients/${clientId}`);
            }
        },
        codeStore: {
            save: (oauth2Code) => {
                return put(bucket, `codes/${oauth2Code.code}`, oauth2Code);
            },
            fetchByCode: (code) => {
                return get(bucket, `codes/${code}`);
            },
            removeByCode: (code) => {
                return remove(bucket, `codes/${code}`);
            }
        },
        refreshTokenStore: tokenStore('refresh-token', bucket),
        tokenStore: tokenStore('token', bucket),
        userStore: {
            save: (user) => {
                return put(bucket, `users/${user.id}`, user)
            },
            fetchById: (userId) => {
                return get(bucket, `user/${userId}`);
            }
        }
    }
}
