{{config(
        alias = 'bridges_optimism',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["rantum"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
      ('optimism', 0x4200000000000000000000000000000000000010, 'Optimism Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x3f87ff1de58128ef8fcb4c807efd776e1ac72e51, 'Optimism Bridge 2', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x99c9fc46f92e8a1c0dec1b1747d010903e884be1, 'Optimism Gateway', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x9d39fc627a6d9d9f8c831c16995b209548cc3401, 'Celer Network Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0xe7351fd770a37282b91d153ee690b63579d6dd7f, 'Din Destination', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x3c2269811836af69497e5f486a85d7316753cf62, 'Layer Zero Optimism', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0xc10ef9f491c9b59f936957026020c321651ac078, 'Multichain anyCall v6', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0xdc42728b0ea910349ed3c6e1c9dc06b5fb591f98, 'Multichain Router v6', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0xaf41a65f786339e7911f4acdad6bd49426f2dc6b, 'Synapse', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x83f6244bd87662118d96d9a6d44f09dfff14b30e, 'Hop', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x7191061d5d4c60f598214cc6913502184baddf18, 'Hop', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0xa81d244a1814468c734e5b4101f7b9c0c577a8fc, 'Hop', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x46ae9bab8cea96610807a275ebd36f8e916b5c61, 'Hop', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x4200000000000000000000000000000000000007, 'Optimism Cross Domain Messenger', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier'),
      ('optimism', 0x36BDE71C97B33Cc4729cf772aE268934f7AB70B2, 'Optimism Aliased L1 Cross Domain Messenger', 'bridge', 'rantum', 'static', DATE '2023-11-17', now(), 'bridges_polygon', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)