(function(){
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  function bufToB64(buf){
    return btoa(String.fromCharCode(...new Uint8Array(buf)));
  }
  function b64ToBuf(b64){
    const bin = atob(b64);const buf=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)buf[i]=bin.charCodeAt(i);return buf.buffer;
  }

  async function deriveMasterKey(passphrase){
    const salt = encoder.encode('notion-e2ee');
    const keyMat = await crypto.subtle.importKey('raw', encoder.encode(passphrase),'PBKDF2',false,['deriveKey']);
    return crypto.subtle.deriveKey({name:'PBKDF2',salt,iterations:100000,hash:'SHA-256'},keyMat,{name:'AES-GCM',length:256},true,['encrypt','decrypt']);
  }

  let cachedMasterKey=null;
  async function getMasterKey(){
    if(cachedMasterKey) return cachedMasterKey;
    const stored = localStorage.getItem('e2eeMasterKey');
    if(stored){
      const raw=b64ToBuf(stored);
      cachedMasterKey=await crypto.subtle.importKey('raw',raw,{name:'AES-GCM'},true,['encrypt','decrypt']);
      return cachedMasterKey;
    }
    const passphrase=prompt('Enter encryption passphrase');
    if(!passphrase) throw new Error('Passphrase required');
    cachedMasterKey=await deriveMasterKey(passphrase);
    const raw=await crypto.subtle.exportKey('raw',cachedMasterKey);
    localStorage.setItem('e2eeMasterKey', bufToB64(raw));
    return cachedMasterKey;
  }

  async function encryptFileKey(masterKey, rawFileKey){
    const iv=crypto.getRandomValues(new Uint8Array(12));
    const ciphertext=await crypto.subtle.encrypt({name:'AES-GCM',iv},masterKey,rawFileKey);
    return {encryptedKey:ciphertext, iv};
  }

  async function decryptFileKey(masterKey, encryptedKey, iv){
    return crypto.subtle.decrypt({name:'AES-GCM',iv},masterKey,encryptedKey);
  }

  async function encryptStream(file, progressCallback){
    const fileKey=await crypto.subtle.generateKey({name:'AES-GCM',length:256},true,['encrypt','decrypt']);
    const ivSeed=crypto.getRandomValues(new Uint8Array(8));
    const chunkSize=64*1024;
    let offset=0; let index=0;
    const ivList=[]; const macList=[]; const chunkLens=[];

    const stream=new ReadableStream({
      async pull(controller){
        if(offset>=file.size){controller.close();return;}
        const end=Math.min(offset+chunkSize,file.size);
        const chunk=new Uint8Array(await file.slice(offset,end).arrayBuffer());
        const iv=new Uint8Array(12);
        iv.set(ivSeed,0);
        new DataView(iv.buffer).setUint32(8,index,false);
        const encrypted=new Uint8Array(await crypto.subtle.encrypt({name:'AES-GCM',iv},fileKey,chunk));
        const mac=encrypted.slice(encrypted.length-16);
        ivList.push(bufToB64(iv));
        macList.push(bufToB64(mac));
        chunkLens.push(encrypted.length);
        controller.enqueue(encrypted);
        offset=end; index++;
        if(progressCallback){progressCallback((offset/file.size)*100, offset);}
      }
    });

    const manifest={
      algo:'AES-256-GCM',
      iv_list:ivList,
      mac_list:macList,
      chunk_lengths:chunkLens,
      total_size:file.size
    };
    return {stream,fileKey,manifest};
  }

  async function downloadEncrypted(url, metadata){
    const resp=await fetch(url);
    const encrypted=new Uint8Array(await resp.arrayBuffer());
    const masterKey=await getMasterKey();
    const rawFileKey=await decryptFileKey(masterKey,b64ToBuf(metadata.encrypted_file_key),b64ToBuf(metadata.file_key_iv));
    const fileKey=await crypto.subtle.importKey('raw',rawFileKey,{name:'AES-GCM'},false,['decrypt']);
    let offset=0; const parts=[];
    for(let i=0;i<metadata.iv_list.length;i++){
      const iv=b64ToBuf(metadata.iv_list[i]);
      const len=metadata.chunk_lengths[i];
      const chunk=encrypted.slice(offset, offset+len);
      const decrypted=await crypto.subtle.decrypt({name:'AES-GCM',iv:new Uint8Array(iv)}, fileKey, chunk);
      parts.push(new Uint8Array(decrypted));
      offset+=len;
    }
    const blob=new Blob(parts,{type: resp.headers.get('Content-Type')||'application/octet-stream'});
    const a=document.createElement('a');
    a.href=URL.createObjectURL(blob);
    a.download=metadata.original_filename||'download';
    document.body.appendChild(a);a.click();a.remove();
  }

  window.E2EE={
    getMasterKey,
    encryptFileKey,
    decryptFileKey,
    encryptStream,
    downloadEncrypted,
    bufToB64,
    b64ToBuf
  };
})();
