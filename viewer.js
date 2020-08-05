const mediasoup = require('mediasoup-client');
const socketClient = require('socket.io-client');
const socketPromise = require('./lib/socket.io-promise').promise;
const config = require('./config.example.js');

const hostname = window.location.hostname;

let device;
let socket;
let producer;

const $ = document.querySelector.bind(document);
const $fsSubscribe = $('#fs_subscribe');
const $btnSubscribe = $('#btn_subscribe');
const $txtConnection = $('#connection_status');
const $txtSubscription = $('#sub_status');
let $txtPublish;

$btnSubscribe.addEventListener('click', subscribe);

connect()

async function connect() {
  // $btnConnect.disabled = true;
  $txtConnection.innerHTML = 'Connecting...';

  const opts = {
    path: '/server',
    transports: ['websocket'],
  };

  const serverUrl = `https://${hostname}:${config.listenPort}`;
  socket = socketClient(serverUrl, opts);
  socket.request = socketPromise(socket);

  socket.on('connect', async () => {
    $txtConnection.innerHTML = 'Connected';
    $fsSubscribe.disabled = false;

    const data = await socket.request('getRouterRtpCapabilities');
    await loadDevice(data);
    subscribe()
  });

  socket.on('disconnect', () => {
    $txtConnection.innerHTML = 'Disconnected';
    $fsSubscribe.disabled = true;
  });

  socket.on('connect_error', (error) => {
    console.error('could not connect to %s%s (%s)', serverUrl, opts.path, error.message);
    $txtConnection.innerHTML = 'Connection failed';
    // $btnConnect.disabled = false;
  });

  socket.on('newProducer', () => {
    $fsSubscribe.disabled = false;
    subscribe()
  });
}

async function loadDevice(routerRtpCapabilities) {
  try {
    device = new mediasoup.Device();
  } catch (error) {
    if (error.name === 'UnsupportedError') {
      console.error('browser not supported');
    }
  }
  await device.load({ routerRtpCapabilities });
}



async function subscribe() {
  const data = await socket.request('createConsumerTransport', {
    forceTcp: false,
  });
  if (data.error) {
    console.error(data.error);
    return;
  }

  const transport = device.createRecvTransport(data);
  transport.on('connect', ({ dtlsParameters }, callback, errback) => {
    socket.request('connectConsumerTransport', {
      transportId: transport.id,
      dtlsParameters
    })
      .then(callback)
      .catch(errback);
  });

  transport.on('connectionstatechange', async (state) => {
    switch (state) {
      case 'connecting':
        $txtSubscription.innerHTML = 'subscribing...';
        $fsSubscribe.disabled = true;
        break;

      case 'connected':
        document.querySelector('#remote_video').srcObject = await stream;
        await socket.request('resume');
        $fsSubscribe.disabled = true;
        $txtSubscription.innerHTML = 'subscribed';
        break;

      case 'failed':
        transport.close();
        $txtSubscription.innerHTML = 'failed';
        $fsSubscribe.disabled = false;
        break;

      default: break;
    }
  });

  const stream = consume(transport);
}

async function consume(transport) {
  const { rtpCapabilities } = device;
  const data = await socket.request('consume', { rtpCapabilities });
  const {
    producerId,
    id,
    kind,
    rtpParameters,
  } = data;

  let codecOptions = {};
  const consumer = await transport.consume({
    id,
    producerId,
    kind,
    rtpParameters,
    codecOptions,
  });
  console.log('Receiving a new remote MediaStreamTrack:', consumer.kind);

  const stream = new MediaStream();
  stream.addTrack(consumer.track);
  return stream;
}
